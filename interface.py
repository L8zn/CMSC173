# interface.py
import sys
import threading
import os
import time
from node import Node
from utils import node_info # Make sure node_info displays data/replica stores usefully
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

# Prompt for IP and port
ip = str(input("Ip Address: "))
port = int(input("Port number: "))
node = Node(ip, port)

def cli_loop():
    print("\nWelcome to the Chord Hierarchical (Path-Based) DFS CLI!")
    while True:
        with patch_stdout():
            user_input = prompt(
                f"\n=== Node {node.id} CLI ({node.ip}:{node.port}) ===\n"
                "Commands:\n"
                "  JOIN <ip> <port>         - Join the Chord ring\n"
                "  UPLOAD <local> <remote>  - Upload file to remote path (e.g., /dir/file.txt)\n"
                "  GET <remote> <local>     - Get file from remote path\n"
                "  MKDIR <remote_path>      - Create a directory\n"
                "  LISTDIR <remote_path>    - List contents of a directory (basic)\n"
                # "  STORE <key> <value>    - Store simple key-value (legacy)\n"
                # "  LOOKUP <key>           - Lookup simple key-value (legacy)\n"
                "  LEAVE                    - Leave the Chord ring gracefully\n"
                "  INFO                     - Display node status and finger table\n"
                "  EXIT                     - Stop this node and exit\n"
                "Enter command: "
            ).strip()

        parts = user_input.split(' ', 2) # Split max 2 times for UPLOAD/GET
        if not parts:
            continue

        command = parts[0].upper()
        args = []
        if len(parts) > 1:
            args = parts[1:] # Might contain paths with spaces if not handled carefully

        # --- Simplified parsing for commands ---
        full_input_parts = user_input.split()

        try:
            if command == "JOIN":
                if len(full_input_parts) != 3:
                    print("Usage: JOIN <ip> <port>")
                    continue
                try:
                    known_ip = full_input_parts[1]
                    known_port = int(full_input_parts[2])
                    node.join(known_ip, known_port)
                except ValueError:
                    print("Error: Port must be a number.")

            elif command == "UPLOAD":
                # Need careful parsing if paths have spaces (assume no spaces for now)
                if len(full_input_parts) != 3:
                    print("Usage: UPLOAD <local_filepath> <remote_filepath>")
                    print("       (Remote filepath must start with /)")
                    continue
                local_path = full_input_parts[1]
                remote_path = full_input_parts[2]
                if not remote_path.startswith('/'):
                     print("Error: Remote filepath must start with /")
                     continue
                if not os.path.exists(local_path):
                     print(f"Error: Local file not found: {local_path}")
                     continue
                if not os.path.isfile(local_path):
                    print(f"Error: Local path is not a file: {local_path}")
                    continue
                print(f"Preparing to upload '{local_path}' to '{remote_path}'...")
                node.upload_file(local_path, remote_path)

            elif command == "GET":
                if len(full_input_parts) != 3:
                    print("Usage: GET <remote_filepath> <local_destination>")
                    print("       (Remote filepath must start with /)")
                    continue
                remote_path = full_input_parts[1]
                local_dest = full_input_parts[2]
                if not remote_path.startswith('/'):
                     print("Error: Remote filepath must start with /")
                     continue
                # Basic validation for local destination
                dest_dir = os.path.dirname(local_dest)
                if dest_dir and not os.path.exists(dest_dir):
                    try: os.makedirs(dest_dir); print(f"Created directory: {dest_dir}")
                    except OSError as e: print(f"Error creating {dest_dir}: {e}"); continue
                elif os.path.isdir(local_dest):
                     print(f"Error: Local destination cannot be a directory: {local_dest}")
                     continue

                print(f"Requesting remote file '{remote_path}' to save at '{local_dest}'...")
                node.retrieve_file(remote_path, local_dest)

            elif command == "MKDIR":
                if len(full_input_parts) != 2:
                     print("Usage: MKDIR <remote_path>")
                     print("       (Remote path must start with /)")
                     continue
                remote_path = full_input_parts[1]
                if not remote_path.startswith('/'):
                     print("Error: Remote path must start with /")
                     continue
                print(f"Requesting to create directory '{remote_path}'...")
                node.make_directory(remote_path)

            elif command == "LISTDIR":
                 if len(full_input_parts) != 2:
                     print("Usage: LISTDIR <remote_path>")
                     print("       (Remote path must start with /)")
                     continue
                 remote_path = full_input_parts[1]
                 if not remote_path.startswith('/'):
                     print("Error: Remote path must start with /")
                     continue
                 print(f"Requesting listing for directory '{remote_path}'...")
                 node.list_directory(remote_path)

            # elif command == "STORE": ... # Keep or remove legacy
            # elif command == "LOOKUP": ... # Keep or remove legacy

            elif command == "LEAVE":
                node.leave()
                print("Node has left the ring. Use EXIT to terminate.")

            elif command == "INFO":
                node_info(node) # Consider updating node_info to show path types

            elif command == "EXIT":
                print("Initiating shutdown...")
                node.leave()
                time.sleep(1.5) # Give leave messages time
                node.stop_event.set()
                try: # Attempt to unblock socket
                    node.sock.sendto(b'', (node.ip, node.port))
                except Exception: pass
                node.sock.close()
                print("Node stopped. Exiting.")
                # Use os._exit to force exit even if threads hang
                os._exit(0) # Force exit

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

    # Keep main thread alive until CLI signals exit (or handle Ctrl+C)
    try:
         cli_thread.join()
    except KeyboardInterrupt:
         print("\nCtrl+C detected. Initiating shutdown...")
         node.leave()
         time.sleep(1.5)
         node.stop_event.set()
         try: node.sock.sendto(b'', (node.ip, node.port))
         except Exception: pass
         node.sock.close()
         print("Node stopped. Exiting.")
         os._exit(1) # Force exit