import sys
from node import Node
from utils import debug_node_info

# Prompt for IP address and port
ip = input("Enter IP address: ")
port = int(input("Enter Port number: "))

# Initialize the node
node = Node(ip, port)

# Command loop
while True:
    command = input("Enter command (JOIN, STORE, LOOKUP, LEAVE, INFO, EXIT): ").strip().upper()

    if command == "JOIN":
        known_ip = input("Enter known node IP: ")
        known_port = int(input("Enter known node Port: "))
        node.join(known_ip, known_port)

    elif command == "STORE":
        key = input("Enter key to store: ")
        value = input("Enter value: ")
        node.store(key, value)

    elif command == "LOOKUP":
        key = input("Enter key to lookup: ")
        node.lookup(key)

    elif command == "LEAVE":
        """ Simulates a node gracefully leaving the Chord network. """
        print(f"Node {node.id} leaving the network...")
        sys.exit()

    elif command == "INFO":
        debug_node_info(node)

    elif command == "EXIT":
        """ Forcefully closes the node interface without any Chord-specific cleanup. """
        print("Exiting...")
        sys.exit()

    else:
        print("Invalid command. Available commands: JOIN, STORE, LOOKUP, LEAVE, INFO, EXIT")
