import socket
import threading
import time
from chord import Chord
from utils import hash_function, debug_node_info, in_range

class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id = hash_function(f"{ip}:{port}")
        # Initially, the node is alone in the ring.
        self.successor = {"ip": ip, "port": port, "id": self.id}
        self.predecessor = None
        self.data_store = {}  # Key-value storage

        # Temporary storage for GET_PREDECESSOR reply during stabilization.
        self.temp_predecessor = None
        # For detecting a failed predecessor.
        self.last_predecessor_heartbeat = time.time()

        # Chord protocol integration.
        self.chord = Chord(self)

        # UDP Socket setup.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))

        # Start background threads.
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.stabilize, daemon=True).start()
        threading.Thread(target=self.fix_fingers, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()

    def listen(self):
        """Continuously listen for incoming UDP messages and handle them."""
        print(f"Node {self.id} listening on {self.ip}:{self.port}")
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                print(f"Node {self.id} received message from {addr}: {message}")
                self.handle_message(message, addr)
            except Exception as e:
                print(f"Error in listening: {e}")

    def send_message(self, target_ip, target_port, message):
        """Send a UDP message to the specified target."""
        self.sock.sendto(message.encode(), (target_ip, target_port))

    def handle_message(self, message, addr):
        """Process an incoming message based on its command type."""
        parts = message.split()
        if not parts:
            return
        command = parts[0]

        if command == "FIND_SUCCESSOR":
            # A request to find the successor of a given key ID.
            key_id = int(parts[1])
            successor = self.chord.find_successor(key_id)
            if successor:
                self.send_message(addr[0], addr[1],
                                  f"SUCCESSOR {successor['ip']} {successor['port']} {successor['id']}")
        elif command == "SUCCESSOR":
            # Reply to a FIND_SUCCESSOR: update this node's successor pointer.
            successor_ip = parts[1]
            successor_port = int(parts[2])
            successor_id = int(parts[3])
            self.successor = {"ip": successor_ip, "port": successor_port, "id": successor_id}
            print(f"Node {self.id} updated its successor to: {self.successor}")
            # Notify the new successor of our presence.
            self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
            self.chord.update_finger_table()
        elif command == "NOTIFY":
            # A node is telling us that it might be our predecessor.
            potential_predecessor_id = int(parts[1])
            if self.predecessor is None or in_range(potential_predecessor_id, self.predecessor["id"], self.id):
                self.predecessor = {"ip": addr[0], "port": addr[1], "id": potential_predecessor_id}
                print(f"Node {self.id} updated its predecessor to: {self.predecessor}")
        elif command == "GET_PREDECESSOR":
            # Request: send our predecessor information.
            if self.predecessor:
                reply = f"PREDECESSOR {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}"
            else:
                reply = "PREDECESSOR NONE"
            self.send_message(addr[0], addr[1], reply)
        elif command == "PREDECESSOR":
            # Reply to a GET_PREDECESSOR: store it temporarily for stabilization.
            if parts[1] == "NONE":
                self.temp_predecessor = None
            else:
                pred_ip = parts[1]
                pred_port = int(parts[2])
                pred_id = int(parts[3])
                self.temp_predecessor = {"ip": pred_ip, "port": pred_port, "id": pred_id}
        elif command == "STORE":
            # Store a key-value pair in the responsible node.
            key = parts[1]
            value = parts[2]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor["id"] == self.id:
                self.data_store[key] = value
                print(f"Node {self.id} stored key-value: {key}: {value}")
            else:
                self.send_message(successor["ip"], successor["port"], message)
        elif command == "LOOKUP":
            # Lookup a key and return its value.
            key = parts[1]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor["id"] == self.id:
                value = self.data_store.get(key, "NOT_FOUND")
                self.send_message(addr[0], addr[1], f"RESULT {key} {value}")
            else:
                self.send_message(successor["ip"], successor["port"], message)
        elif command == "PING":
            # Reply to a liveness check.
            self.send_message(addr[0], addr[1], "PONG")
            # If the sender is our predecessor, update heartbeat.
            if (self.predecessor and addr[0] == self.predecessor["ip"] 
                    and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        elif command == "PONG":
            # Update heartbeat from our predecessor.
            if (self.predecessor and addr[0] == self.predecessor["ip"] 
                    and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        elif command == "RESULT":
            # Handle lookup results.
            key = parts[1]
            value = parts[2]
            print(f"Lookup result for {key}: {value}")

    def join(self, known_node_ip, known_node_port):
        """
        Join the Chord ring.
        If the node is the first node (i.e. joining itself), it initializes its own ring.
        Otherwise, it contacts a known node to locate its successor.
        """
        if known_node_ip == self.ip and known_node_port == self.port:
            # This node is the first in the ring.
            self.predecessor = None
            self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
            print(f"Node {self.id} initialized as the first node in the ring.")
        else:
            print(f"Node {self.id} joining ring via {known_node_ip}:{known_node_port}")
            self.send_message(known_node_ip, known_node_port, f"FIND_SUCCESSOR {self.id}")
            # The SUCCESSOR reply (handled asynchronously) will update our successor pointer.

    def stabilize(self):
        """
        Periodically verify and update the successor pointer.
        This routine works as follows:
          1. If the node's successor pointer still equals itself even though a valid predecessor exists,
             then update the successor pointer immediately.
          2. Otherwise, query the current successor for its predecessor (via GET_PREDECESSOR)
             and, if that node lies between this node and its successor, update the successor pointer.
          3. Finally, notify the (possibly updated) successor about this node.
        """
        while True:
            # If the node is not alone (has a non-self predecessor) but its successor is still self,
            # update the successor pointer immediately.
            if self.successor["id"] == self.id and self.predecessor is not None and self.predecessor["id"] != self.id:
                self.successor = self.predecessor
                print(f"Node {self.id} updated its successor to its predecessor: {self.successor}")
            else:
                # Normal stabilization: if our successor is not self, ask for its predecessor.
                if self.successor["id"] != self.id:
                    self.temp_predecessor = None
                    self.send_message(self.successor["ip"], self.successor["port"], "GET_PREDECESSOR")
                    time.sleep(0.5)  # Wait briefly for a reply.
                    x = self.temp_predecessor
                    if x and in_range(x["id"], self.id, self.successor["id"]):
                        self.successor = x
                        print(f"Node {self.id} updated its successor to {self.successor} via stabilization")
            # Notify the (possibly updated) successor about ourselves.
            self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
            self.chord.update_finger_table()
            time.sleep(5)

    def fix_fingers(self):
        """
        Periodically refresh the finger table entries.
        (Here we update the whole table at once for simplicity.)
        """
        while True:
            self.chord.update_finger_table()
            time.sleep(5)

    def check_predecessor(self):
        """
        Periodically check whether the predecessor is still alive.
        If no heartbeat is received within a threshold, remove the predecessor.
        """
        while True:
            if self.predecessor:
                self.send_message(self.predecessor["ip"], self.predecessor["port"], "PING")
                if time.time() - self.last_predecessor_heartbeat > 15:
                    print(f"Node {self.id} detected failed predecessor {self.predecessor}")
                    self.predecessor = None
            time.sleep(5)

    def store(self, key, value):
        """Initiate a store command on this node."""
        self.send_message(self.ip, self.port, f"STORE {key} {value}")

    def lookup(self, key):
        """Initiate a lookup command on this node."""
        self.send_message(self.ip, self.port, f"LOOKUP {key}")

# --- For testing purposes ---
if __name__ == "__main__":
    node1 = Node("127.0.0.1", 5000)
    node2 = Node("127.0.0.1", 5001)
    
    time.sleep(5)
    # debug_node_info(node1)
    # debug_node_info(node2)

    time.sleep(1)
    node2.join("127.0.0.1", 5000)
    time.sleep(5)

    # debug_node_info(node1)
    # debug_node_info(node2)

    node3 = Node("127.0.0.1", 5002)

    time.sleep(5)
    # debug_node_info(node3)

    time.sleep(1)
    node3.join("127.0.0.1", 5000)
    time.sleep(5)

    node4 = Node("127.0.0.1", 5003)

    time.sleep(5)
    # debug_node_info(node4)

    time.sleep(1)
    node4.join("127.0.0.1", 5000)
    time.sleep(5)
    time.sleep(5)

    debug_node_info(node1)
    debug_node_info(node2)
    debug_node_info(node3)
    debug_node_info(node4)
