import socket
import threading
import time
from chord import Chord
from utils import hash_function, node_info, in_range

class Node:
    def __init__(self, ip, port, r=3):  # r = number of successors for fault tolerance and replication
        self.ip = ip
        self.port = port
        self.id = hash_function(f"{ip}:{port}")
        # Initially, the node is alone in the ring.
        self.successor = {"ip": ip, "port": port, "id": self.id}
        self.predecessor = None
        self.data_store = {}     # Primary key-value store.
        self.replica_store = {}  # Replicated key-value store.

        # Successor list for fault tolerance and replication.
        self.r = r
        self.successor_list = [self.successor]  # Initially only self.

        # Temporary storage for GET_PREDECESSOR reply during stabilization.
        self.temp_predecessor = None
        # For detecting a failed predecessor.
        self.last_predecessor_heartbeat = time.time()

        # Chord protocol integration.
        self.chord = Chord(self)

        # UDP Socket setup.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))

        # Event to signal shutdown.
        self.stop_event = threading.Event() 
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.node_stabilize, daemon=True).start()
        threading.Thread(target=self.fix_fingers, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()

    def listen(self):
        """Continuously listen for incoming UDP messages and handle them."""
        print(f"Node {self.id} listening on {self.ip}:{self.port}")
        while not self.stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(1024)
                message = data.decode()
                # print(f"Node {self.id} received message from {addr}: {message}")
                self.handle_message(message, addr)
            except OSError as e:
                if not self.stop_event.is_set():
                    print(f"Error in listening: {e}")
            except Exception as e:
                if not self.stop_event.is_set():
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
            key_id = int(parts[1])
            successor = self.chord.find_successor(key_id)
            if successor:
                self.send_message(addr[0], addr[1],
                                  f"SUCCESSOR {successor['ip']} {successor['port']} {successor['id']}")
        elif command == "SUCCESSOR":
            successor_ip = parts[1]
            successor_port = int(parts[2])
            successor_id = int(parts[3])
            self.successor = {"ip": successor_ip, "port": successor_port, "id": successor_id}
            if self.successor_list:
                self.successor_list[0] = self.successor
            else:
                self.successor_list = [self.successor]
            print(f"Node {self.id} updated its successor to: {self.successor}")
            self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
            self.chord.update_finger_table()
        elif command == "NOTIFY":
            potential_predecessor_id = int(parts[1])
            if self.predecessor is None or in_range(potential_predecessor_id, self.predecessor["id"], self.id):
                self.predecessor = {"ip": addr[0], "port": addr[1], "id": potential_predecessor_id}
                print(f"Node {self.id} updated its predecessor to: {self.predecessor}")
        elif command == "GET_PREDECESSOR":
            if self.predecessor:
                reply = f"PREDECESSOR {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}"
            else:
                reply = "PREDECESSOR NONE"
            self.send_message(addr[0], addr[1], reply)
        elif command == "PREDECESSOR":
            if parts[1] == "NONE":
                self.temp_predecessor = None
            else:
                pred_ip = parts[1]
                pred_port = int(parts[2])
                pred_id = int(parts[3])
                self.temp_predecessor = {"ip": pred_ip, "port": pred_port, "id": pred_id}
        elif command == "GET_SUCCESSOR_LIST":
            self.chord.prune_successor_list()  # Prune before replying.
            list_str = " ".join(f"{entry['ip']} {entry['port']} {entry['id']}" for entry in self.successor_list)
            reply = f"SUCCESSOR_LIST {list_str}"
            self.send_message(addr[0], addr[1], reply)
        elif command == "SUCCESSOR_LIST":
            new_list = []
            num_entries = (len(parts) - 1) // 3
            for i in range(num_entries):
                entry_ip = parts[1 + 3*i]
                entry_port = int(parts[2 + 3*i])
                entry_id = int(parts[3 + 3*i])
                new_list.append({"ip": entry_ip, "port": entry_port, "id": entry_id})
            if new_list:
                self.successor_list = [self.successor]  # Ensure immediate successor is first.
                for entry in new_list:
                    if entry["id"] != self.id and len(self.successor_list) < self.r:
                        self.successor_list.append(entry)
                print(f"Node {self.id} updated its successor list to: {self.successor_list}")
        elif command == "UPDATE_PREDECESSOR_TO":
            new_pred_ip = parts[1]
            new_pred_port = int(parts[2])
            new_pred_id = int(parts[3])
            self.predecessor = {"ip": new_pred_ip, "port": new_pred_port, "id": new_pred_id}
            print(f"Node {self.id} updated predecessor to Node {self.predecessor['id']}")
        elif command == "UPDATE_SUCCESSOR_TO":
            new_succ_ip = parts[1]
            new_succ_port = int(parts[2])
            new_succ_id = int(parts[3])
            self.successor = {"ip": new_succ_ip, "port": new_succ_port, "id": new_succ_id}
            if self.successor_list:
                self.successor_list[0] = self.successor
            print(f"Node {self.id} updated successor to Node {self.successor['id']}")
        elif command == "STORE":
            key = parts[1]
            value = parts[2]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor["id"] == self.id:
                self.data_store[key] = value
                print(f"Node {self.id} stored key-value: {key}: {value}")
                for s in self.successor_list[1:]:
                    self.send_message(s["ip"], s["port"], f"REPLICATE {key} {value}")
            else:
                self.send_message(successor["ip"], successor["port"], message)
        elif command == "REPLICATE":
            key = parts[1]
            value = parts[2]
            self.replica_store[key] = value
            print(f"Node {self.id} stored replicated key-value: {key}: {value}")
        elif command == "LOOKUP":
            key = parts[1]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor["id"] == self.id:
                value = self.data_store.get(key, None)
                if value is None:
                    value = self.replica_store.get(key, "NOT_FOUND")
                self.send_message(addr[0], addr[1], f"RESULT {key} {value}")
            else:
                self.send_message(successor["ip"], successor["port"], message)
        elif command == "PING":
            self.send_message(addr[0], addr[1], "PONG")
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        elif command == "PONG":
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        elif command == "RESULT":
            key = parts[1]
            value = parts[2]
            print(f"Lookup result for {key}: {value}")

    def join(self, known_node_ip, known_node_port):
        if known_node_ip == self.ip and known_node_port == self.port:
            self.predecessor = None
            self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
            self.successor_list = [self.successor]
            print(f"Node {self.id} initialized as the first node in the ring.")
        else:
            print(f"Node {self.id} joining ring via {known_node_ip}:{known_node_port}")
            self.send_message(known_node_ip, known_node_port, f"FIND_SUCCESSOR {self.id}")

    def node_stabilize(self):
        while not self.stop_event.is_set():
            # Prune the successor list before using it.
            self.chord.prune_successor_list()
            self.chord.stabilize()
            # Update successor list from our immediate successor.
            self.chord.update_successor_list()
            self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
            self.chord.update_finger_table()
            time.sleep(5)

    def fix_fingers(self):
        while not self.stop_event.is_set():
            self.chord.update_finger_table()
            time.sleep(5)

    def check_predecessor(self):
        while not self.stop_event.is_set():
            if self.predecessor:
                # If the predecessor is self, we don't need to ping
                if self.predecessor["id"] == self.id:
                    self.last_predecessor_heartbeat = time.time()
                else:
                    self.send_message(self.predecessor["ip"], self.predecessor["port"], "PING")
                    if time.time() - self.last_predecessor_heartbeat > 15:
                        print(f"Node {self.id} detected failed predecessor {self.predecessor}")
                        # If the predecessor fails, and if this node is alone,
                        # then we update our predecessor to self.
                        if self.successor["id"] == self.id:
                            self.predecessor = {"ip": self.ip, "port": self.port, "id": self.id}
                        else:
                            self.predecessor = None
            time.sleep(5)

    def store(self, key, value):
        self.send_message(self.ip, self.port, f"STORE {key} {value}")

    def lookup(self, key):
        self.send_message(self.ip, self.port, f"LOOKUP {key}")

    def leave(self):
        print(f"Node {self.id} leaving the network.")
        if self.successor and self.successor["id"] != self.id:
            for key, value in self.data_store.items():
                self.send_message(self.successor["ip"], self.successor["port"], f"STORE {key} {value}")
            for key, value in self.replica_store.items():
                self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE {key} {value}")
            print(f"Node {self.id} transferred data to successor {self.successor['id']}")
            self.send_message(self.successor["ip"], self.successor["port"], 
                              f"UPDATE_PREDECESSOR_TO {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}")
        if self.predecessor and self.predecessor["id"] != self.id:
            self.send_message(self.predecessor["ip"], self.predecessor["port"], 
                              f"UPDATE_SUCCESSOR_TO {self.successor['ip']} {self.successor['port']} {self.successor['id']}")
        time.sleep(0.5)
        self.stop_event.set()
        print(f"Node {self.id} has exited the Chord ring.")
        self.sock.close()

# --- For testing purposes ---
if __name__ == "__main__":
    node1 = Node("127.0.0.1", 5000)
    node2 = Node("127.0.0.1", 5001)
    
    time.sleep(5)
    time.sleep(1)
    node2.join("127.0.0.1", 5000)
    time.sleep(5)
    node3 = Node("127.0.0.1", 5002)
    time.sleep(5)
    time.sleep(1)
    node3.join("127.0.0.1", 5000)
    time.sleep(5)
    node4 = Node("127.0.0.1", 5003)
    time.sleep(5)
    time.sleep(1)
    node4.join("127.0.0.1", 5000)
    time.sleep(5)
    node_info(node1)
    node_info(node2)
    node_info(node3)
    node_info(node4)
    node4.leave()
    time.sleep(10)
    node_info(node1)
    node_info(node2)
    node_info(node3)
