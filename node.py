import socket
import threading
import time
from chord import Chord
from utils import hash_function, debug_node_info
class RemoteNode:
    def __init__(self, ip, port, node_id):
        self.ip = ip
        self.port = port
        self.id = node_id
class Node:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.id = hash_function(f"{ip}:{port}")
        self.successor = self  # Initially, the node is its own successor
        self.predecessor = None
        self.data_store = {}  # Key-value storage

        # Chord protocol integration
        self.chord = Chord(self)

        # UDP Socket setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((ip, port))

        # Start listener and stabilization threads
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.stabilize, daemon=True).start()

    def listen(self):
        print(f"Node {self.id} listening on {self.ip}:{self.port}")
        while True:
            data, addr = self.sock.recvfrom(1024)
            message = data.decode()
            print(f"Received message from {addr}: {message}")
            self.handle_message(message, addr)

    def send_message(self, target_ip, target_port, message):
        self.sock.sendto(message.encode(), (target_ip, target_port))

    def handle_message(self, message, addr):
        parts = message.split()
        command = parts[0]

        if command == "JOIN":
            new_node_id = int(parts[1])

            print(f"Processing JOIN for Node ID: {new_node_id}")  # Debugging
            if self.successor == self or (self.id < new_node_id < self.successor.id):
                self.send_message(addr[0], addr[1], f"SUCCESSOR {self.successor.ip} {self.successor.port}")
                self.successor = RemoteNode(addr[0], addr[1], new_node_id)  # Update successor
                self.chord.update_finger_table()
            else:
                self.send_message(self.successor.ip, self.successor.port, message)

        elif command == "FIND_SUCCESSOR":
            key_id = int(parts[1])
            successor = self.chord.find_successor(key_id)
            self.send_message(addr[0], addr[1], f"SUCCESSOR {successor.ip} {successor.port}")

        elif command == "NOTIFY":
            potential_predecessor_id = int(parts[1])
            if self.predecessor is None or potential_predecessor_id > self.predecessor.id:
                self.predecessor = Node(addr[0], addr[1])

        elif command == "STORE":
            key, value = parts[1], parts[2]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor.id == self.id:
                self.data_store[key] = value
                print(f"Stored key-value pair: {key}: {value}")
            else:
                self.send_message(successor.ip, successor.port, message)

        elif command == "LOOKUP":
            key = parts[1]
            key_id = hash_function(key)
            successor = self.chord.find_successor(key_id)
            if successor.id == self.id:
                value = self.data_store.get(key, "NOT_FOUND")
                self.send_message(addr[0], addr[1], f"RESULT {key} {value}")
            else:
                self.send_message(successor.ip, successor.port, message)

    def join(self, known_node_ip, known_node_port):
        self.send_message(known_node_ip, known_node_port, f"JOIN {self.id}")

    def stabilize(self):
        while True:
            if self.successor != self:
                self.send_message(self.successor.ip, self.successor.port, f"NOTIFY {self.id}")
                self.chord.update_finger_table()
            time.sleep(5)  # Periodic stabilization every 5 seconds

    def store(self, key, value):
        self.send_message(self.ip, self.port, f"STORE {key} {value}")

    def lookup(self, key):
        self.send_message(self.ip, self.port, f"LOOKUP {key}")

if __name__ == "__main__":
    node1 = Node("127.0.0.1", 5000)
    node2 = Node("127.0.0.1", 5001)
    # debug_node_info(node1)
    # debug_node_info(node2)
    time.sleep(1)
    node2.join("127.0.0.1", 5000)
    time.sleep(1)
    # print("\n")
    # debug_node_info(node1)
    # print("\n")
    # debug_node_info(node2)

    # Example key-value storage and lookup
    # node1.store("username", "alice")
    # node2.lookup("username")
