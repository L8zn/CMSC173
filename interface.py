import sys
import threading
import socket
from node import Node
from utils import debug_node_info

# Function to handle incoming UDP messages
def udp_listener(node):
    while True:
        data, addr = node.sock.recvfrom(1024)
        message = data.decode()

        # Clear the current input line
        sys.stdout.write('\r')
        sys.stdout.flush()

        # Display the incoming message
        print(f"[Received from {addr}] {message}")

        # Redisplay the prompt
        print("\n=== Chord Node CLI ===")
        print("1. JOIN")
        print("2. STORE")
        print("3. LOOKUP")
        print("4. LEAVE")
        print("5. INFO")
        print("6. EXIT")
        sys.stdout.write("Select an option (1-6): ")
        sys.stdout.flush()

        node.handle_message(message, addr)

# Prompt for port
port = int(input("Enter Port number: "))
node = Node("127.0.0.1", port)

# Start the UDP listener thread
threading.Thread(target=udp_listener, args=(node,), daemon=True).start()

# Command loop
while True:
    print("\n=== Chord Node CLI ===")
    print("1. JOIN")
    print("2. STORE")
    print("3. LOOKUP")
    print("4. LEAVE")
    print("5. INFO")
    print("6. EXIT")
    command = input("Select an option (1-6): ").strip()

    if command == "1":  # JOIN
        known_ip = input("Enter known node IP: ")
        known_port = int(input("Enter known node Port: "))
        node.join(known_ip, known_port)

    elif command == "2":  # STORE
        key = input("Enter key to store: ")
        value = input("Enter value: ")
        node.store(key, value)

    elif command == "3":  # LOOKUP
        key = input("Enter key to lookup: ")
        node.lookup(key)

    elif command == "4":  # LEAVE
        print(f"Node {node.id} leaving the network...")
        sys.exit()

    elif command == "5":  # INFO
        debug_node_info(node)

    elif command == "6":  # EXIT
        print("Exiting...")
        sys.exit()

    else:
        print("Invalid option. Please select 1-6.")
