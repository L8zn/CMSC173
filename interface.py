# interface.py
import sys
import threading
import os # <-- Add this
import time
from node import Node
from utils import node_info
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

# Prompt for IP and port (keep existing code)
ip = str(input("Ip Address: "))
port = int(input("Port number: "))
node = Node(ip, port)

def cli_loop():
    print("\nWelcome to the Chord Distributed File System CLI!")
    while True:
        with patch_stdout():
            user_input = prompt(
                f"\n=== Node {node.id} CLI ===\n"
                "Commands:\n"
                "  JOIN <ip> <port>      - Join the Chord ring via a known node\n"
                "  UPLOAD <filepath>     - Upload a file to the DFS\n"
                "  GET <filename> <dest> - Retrieve a file from the DFS\n"
                "  STORE <key> <value>   - Store a simple key-value pair (legacy)\n"
                "  LOOKUP <key>          - Lookup a simple key-value pair (legacy)\n"
                "  LEAVE                 - Leave the Chord ring gracefully\n"
                "  INFO                  - Display node status and finger table\n"
                "  EXIT                  - Stop this node and exit\n"
                "Enter command: "
            ).strip()

        parts = user_input.split()
        if not parts:
            continue

        command = parts[0].upper()
        args = parts[1:]

        try: # Add a general try-except block for robustness
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

            # === NEW: UPLOAD Command ===
            elif command == "UPLOAD":
                if len(args) != 1:
                    print("Usage: UPLOAD <filepath>")
                    continue
                filepath = args[0]
                if not os.path.exists(filepath):
                     print(f"Error: Local file not found: {filepath}")
                     continue
                if not os.path.isfile(filepath):
                    print(f"Error: Path is not a file: {filepath}")
                    continue
                print(f"Preparing to upload '{filepath}'...")
                node.upload_file(filepath)

            # === NEW: GET Command ===
            elif command == "GET":
                if len(args) != 2:
                    print("Usage: GET <filename> <destination_path>")
                    continue
                filename = args[0]
                destination_path = args[1]
                # Basic validation: check if destination directory exists,
                # or handle creation in the node's save logic.
                dest_dir = os.path.dirname(destination_path)
                if dest_dir and not os.path.exists(dest_dir):
                    try:
                        os.makedirs(dest_dir)
                        print(f"Created destination directory: {dest_dir}")
                    except OSError as e:
                         print(f"Error creating destination directory {dest_dir}: {e}")
                         continue
                elif os.path.isdir(destination_path):
                     print(f"Error: Destination path cannot be an existing directory: {destination_path}")
                     continue

                print(f"Requesting file '{filename}' to save at '{destination_path}'...")
                node.retrieve_file(filename, destination_path)

            # --- Keep existing commands ---
            elif command == "STORE": # Legacy K/V
                if len(args) < 2:
                    print("Usage: STORE <key> <value>")
                    continue
                key = args[0]
                value = " ".join(args[1:])
                node.store(key, value)

            elif command == "LOOKUP": # Legacy K/V
                if len(args) != 1:
                    print("Usage: LOOKUP <key>")
                    continue
                key = args[0]
                node.lookup(key)

            elif command == "LEAVE":
                node.leave()
                print("Node has left the ring. You might want to EXIT now.")
                # Note: Leave resets state but doesn't kill the process.

            elif command == "INFO":
                node_info(node) # Make sure node_info still works or update it

            elif command == "EXIT":
                print("Initiating shutdown...")
                node.leave() # Perform graceful leave before exiting
                time.sleep(1) # Give leave messages time to propagate
                node.stop_event.set()
                # Attempt to close socket cleanly
                try:
                    # Send a dummy message to self to unblock recvfrom
                    node.sock.sendto(b'', (node.ip, node.port))
                except Exception:
                    pass # Ignore errors during shutdown
                node.sock.close()
                print("Node stopped. Exiting.")
                sys.exit(0) # Exit cleanly

            else:
                print("Invalid command.")

        except Exception as e:
             print(f"\n--- An error occurred ---")
             print(f"Error: {e}")
             import traceback
             traceback.print_exc()
             print("---------------------------\n")


if __name__ == "__main__":
    # Ensure storage directory exists on startup
    if not os.path.exists(node.storage_dir):
        os.makedirs(node.storage_dir)

    cli_thread = threading.Thread(target=cli_loop, daemon=True)
    cli_thread.start()
    cli_thread.join() # Wait for CLI thread to finish (e.g., on EXIT)