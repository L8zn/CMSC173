import sys
import threading
from node import Node
from utils import debug_node_info
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

# Prompt for port
port = int(input("Enter Port number: "))
node = Node("127.0.0.1", port)

def cli_loop():
    while True:
        with patch_stdout():
            user_input = prompt(
                "\n=== Chord Node CLI ===\n"
                "Commands:\n"
                "  JOIN <ip> <port>\n"
                "  STORE <key> <value>\n"
                "  LOOKUP <key>\n"
                "  LEAVE\n"
                "  INFO\n"
                "  EXIT\n"
                "Enter command: "
            ).strip()

        # Split input into parts (command + arguments)
        parts = user_input.split()
        if not parts:
            continue  # Ignore empty input

        command = parts[0].upper()
        args = parts[1:]

        if command == "JOIN":
            if len(args) != 2:
                print("Usage: JOIN <ip> <port>")
                continue
            try:
                known_ip = args[0]
                known_port = int(args[1])
                node.join(known_ip, known_port)
            except ValueError:
                print("Error: Port must be a number.")

        elif command == "STORE":
            if len(args) < 2:
                print("Usage: STORE <key> <value>")
                continue
            key = args[0]
            value = " ".join(args[1:])  # Allow multi-word values
            node.store(key, value)

        elif command == "LOOKUP":
            if len(args) != 1:
                print("Usage: LOOKUP <key>")
                continue
            key = args[0]
            node.lookup(key)

        elif command == "LEAVE":
            node.leave()

        elif command == "INFO":
            debug_node_info(node)

        elif command == "EXIT":
            print("Exiting...")
            sys.exit()

        else:
            print("Invalid command. Type one of: JOIN, STORE, LOOKUP, LEAVE, INFO, EXIT.")

if __name__ == "__main__":
    cli_thread = threading.Thread(target=cli_loop, daemon=True)
    cli_thread.start()
    cli_thread.join()
