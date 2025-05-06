import socket
import threading
import time
import os # <-- Add this
import base64 # <-- Add this for transferring binary data
import shutil

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
        # === NEW: File Storage Setup ===
        self.storage_dir = f"node_storage_{self.port}"
        if not os.path.exists(self.storage_dir):
            os.makedirs(self.storage_dir)
        print(f"Node {self.id} storing files in: {self.storage_dir}")
        # ==============================

        # data_store/replica_store now track *filenames* managed by this node.
        # The actual content is in self.storage_dir.
        self.data_store = {}     # Maps filename to True/metadata if primary owner
        self.replica_store = {}  # Maps filename to True/metadata if replica owner

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
        # === NEW: Store destination path for pending GET requests ===
        self.pending_get_requests = {} # Maps filename -> local_destination_path
        # ===========================================================

        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.node_stabilize, daemon=True).start()
        threading.Thread(target=self.fix_fingers, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()

    # === NEW: Helper method to get local file path ===
    def _get_local_path(self, remote_path):
        """
        Returns the full local path corresponding to a remote path.
        Handles leading '/' and joins with the storage directory.
        Example: remote_path '/data/file.txt' -> node_storage_XXXX/data/file.txt
        """
        # Remove leading slash if present to avoid issues with os.path.join
        if remote_path.startswith('/'):
            local_relative_path = remote_path[1:]
        else:
            local_relative_path = remote_path # Should ideally start with /

        # Handle potential OS differences if needed (e.g., Windows paths) - simplified for now
        # Replace separators if necessary, though os.path.join handles basics
        # local_relative_path = local_relative_path.replace('/', os.sep)

        return os.path.join(self.storage_dir, local_relative_path)

    # === NEW: Helper method to save file content ===
    def _save_file_content(self, remote_path, content_base64):
        """Saves decoded base64 content to the hierarchical path."""
        local_path = self._get_local_path(remote_path)
        try:
            # Ensure parent directory exists
            parent_dir = os.path.dirname(local_path)
            if not os.path.exists(parent_dir):
                 os.makedirs(parent_dir) # Create intermediate dirs if needed
            content_bytes = base64.b64decode(content_base64)
            with open(local_path, 'wb') as f:
                f.write(content_bytes)
            print(f"Node {self.id} saved file: {local_path} (from remote path {remote_path})")
            return True
        except Exception as e:
            print(f"Node {self.id} ERROR saving file for remote path {remote_path} to {local_path}: {e}")
            return False

    def _read_file_content(self, remote_path):
        """Reads local file content from hierarchical path and returns base64 encoded."""
        local_path = self._get_local_path(remote_path)
        try:
            if os.path.exists(local_path) and os.path.isfile(local_path): # Check if it's a file
                with open(local_path, 'rb') as f:
                    content_bytes = f.read()
                return base64.b64encode(content_bytes).decode('utf-8')
            else:
                print(f"Node {self.id} did not find local file at {local_path} for remote path {remote_path}")
                return None
        except Exception as e:
            print(f"Node {self.id} ERROR reading file at {local_path} for remote path {remote_path}: {e}")
            return None

    # === NEW: Directory Operations ===
    def _create_directory_local(self, remote_path):
        """Creates a directory locally corresponding to the remote path."""
        local_path = self._get_local_path(remote_path)
        try:
            if not os.path.exists(local_path):
                os.makedirs(local_path) # Use makedirs to create intermediate dirs
                print(f"Node {self.id} created directory: {local_path} (from remote path {remote_path})")
                return True
            elif os.path.isdir(local_path):
                 print(f"Node {self.id} directory already exists: {local_path}")
                 return True # Already exists is not an error here
            else:
                 print(f"Node {self.id} ERROR: Path exists but is not a directory: {local_path}")
                 return False
        except Exception as e:
            print(f"Node {self.id} ERROR creating directory {local_path} for remote path {remote_path}: {e}")
            return False

    def _list_directory_local(self, remote_path):
        """Lists contents of a local directory corresponding to the remote path."""
        local_path = self._get_local_path(remote_path)
        try:
            if os.path.exists(local_path) and os.path.isdir(local_path):
                contents = os.listdir(local_path)
                print(f"Node {self.id} listing directory {local_path}: {contents}")
                return contents
            else:
                print(f"Node {self.id} directory not found for listing: {local_path}")
                return None # Indicate not found or not a directory
        except Exception as e:
            print(f"Node {self.id} ERROR listing directory {local_path}: {e}")
            return None


    def listen(self):
        """Continuously listen for incoming UDP messages and handle them."""
        print(f"Node {self.id} listening on {self.ip}:{self.port}")
        while not self.stop_event.is_set():
            try:
                data, addr = self.sock.recvfrom(65535) # Keep increased buffer size
                message = data.decode('utf-8', errors='ignore') # Ignore decoding errors for now
                self.handle_message(message, addr)
            except OSError as e:
                 # Handle socket closed during shutdown gracefully
                if self.stop_event.is_set() and isinstance(e, socket.error) and e.errno == 9: # [Errno 9] Bad file descriptor
                    print("Socket closed.")
                    break
                elif not self.stop_event.is_set():
                    print(f"Error in listening (OSError): {e}")
            except Exception as e:
                if not self.stop_event.is_set():
                    print(f"Error in listening (Exception): {e}")

    def send_message(self, target_ip, target_port, message):
        """Send a UDP message to the specified target."""
        try:
            self.sock.sendto(message.encode('utf-8'), (target_ip, target_port))
        except OSError as e:
            # Handle cases where the socket might be closed during shutdown
            if self.stop_event.is_set() and isinstance(e, socket.error) and e.errno == 9:
                 print(f"Could not send message; socket closed.")
            else:
                 print(f"Error sending message to {target_ip}:{target_port}: {e}")
        except Exception as e:
             print(f"Error sending message to {target_ip}:{target_port}: {e}")


    def handle_message(self, message, addr):
        """Process an incoming message based on its command type."""
        # Split only command first
        command_parts = message.split(' ', 1)
        command = command_parts[0]
        args_str = command_parts[1] if len(command_parts) > 1 else ""

        # --- Chord Core Commands ---
        if command == "FIND_SUCCESSOR":
            # Expecting "FIND_SUCCESSOR key_id"
            try:
                key_id = int(args_str)
                successor = self.chord.find_successor(key_id)
                if successor:
                    self.send_message(addr[0], addr[1],
                                    f"SUCCESSOR {successor['ip']} {successor['port']} {successor['id']}")
            except ValueError:
                print(f"Node {self.id}: Invalid FIND_SUCCESSOR format: {message}")

        elif command == "SUCCESSOR":
            # Expecting "SUCCESSOR ip port id"
            args = args_str.split() # Split the arguments string
            if len(args) == 3:
                try:
                    successor_ip = args[0]
                    successor_port = int(args[1]) # Use args[1]
                    successor_id = int(args[2])   # Use args[2]
                    self.successor = {"ip": successor_ip, "port": successor_port, "id": successor_id}
                    if self.successor_list:
                        self.successor_list[0] = self.successor
                    else:
                        self.successor_list = [self.successor]
                    print(f"Node {self.id} updated its successor to: {self.successor}")
                    # Notify new successor only if it's not self
                    if self.successor['id'] != self.id:
                        self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
                    # Finger table update is handled by fix_fingers thread
                except ValueError:
                    print(f"Node {self.id}: Invalid SUCCESSOR format (port/id): {message}")
            else:
                print(f"Node {self.id}: Invalid SUCCESSOR format (arg count): {message}")

        elif command == "NOTIFY":
            # Expecting "NOTIFY potential_predecessor_id"
            try:
                potential_predecessor_id = int(args_str)
                # Use helper function in_range for clarity
                if self.predecessor is None or \
                in_range(potential_predecessor_id, self.predecessor["id"], self.id, self.chord.m):
                    self.predecessor = {"ip": addr[0], "port": addr[1], "id": potential_predecessor_id}
                    print(f"Node {self.id} updated its predecessor to: {self.predecessor}")
                    self.last_predecessor_heartbeat = time.time() # Update heartbeat
            except ValueError:
                print(f"Node {self.id}: Invalid NOTIFY format: {message}")

        elif command == "GET_PREDECESSOR":
            if self.predecessor:
                reply = f"PREDECESSOR {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}"
            else:
                reply = "PREDECESSOR NONE"
            self.send_message(addr[0], addr[1], reply)

        elif command == "PREDECESSOR":
            # Expecting "PREDECESSOR ip port id" OR "PREDECESSOR NONE"
            args = args_str.split() # Split the arguments string
            if len(args) == 1 and args[0] == "NONE":
                self.temp_predecessor = None
            elif len(args) == 3:
                try:
                    pred_ip = args[0]
                    pred_port = int(args[1]) # Use args[1]
                    pred_id = int(args[2])   # Use args[2]
                    self.temp_predecessor = {"ip": pred_ip, "port": pred_port, "id": pred_id}
                except ValueError:
                    print(f"Node {self.id}: Invalid PREDECESSOR format (port/id): {message}")
                    self.temp_predecessor = None # Reset on error
            else:
                print(f"Node {self.id}: Invalid PREDECESSOR format (arg count): {message}")
                self.temp_predecessor = None # Reset on error

        elif command == "GET_SUCCESSOR_LIST":
            self.chord.prune_successor_list()  # Prune before replying.
            list_to_send = self.successor_list[:self.r]
            list_str = " ".join(f"{entry['ip']} {entry['port']} {entry['id']}" for entry in list_to_send)
            reply = f"SUCCESSOR_LIST {list_str}"
            self.send_message(addr[0], addr[1], reply)

        elif command == "SUCCESSOR_LIST":
            # Expecting "SUCCESSOR_LIST ip1 port1 id1 ip2 port2 id2 ..."
            args = args_str.split() # Split the arguments string
            new_list = []
            if len(args) % 3 == 0: # Check if arguments are in triplets
                try:
                    for i in range(0, len(args), 3):
                        entry_ip = args[i]
                        entry_port = int(args[i + 1]) # Use args[i+1]
                        entry_id = int(args[i + 2])   # Use args[i+2]
                        new_list.append({"ip": entry_ip, "port": entry_port, "id": entry_id})

                    # Update local list, keeping self.successor first and limiting size
                    current_successor = self.successor_list[0] if self.successor_list else self.successor
                    updated_list = [current_successor]
                    for entry in new_list:
                        if entry["id"] != self.id and \
                            len(updated_list) < self.r and \
                            not any(existing["id"] == entry["id"] for existing in updated_list):
                            updated_list.append(entry)
                    self.successor_list = updated_list
                    self.successor = self.successor_list[0] # Ensure self.successor is updated
                    print(f"Node {self.id} updated its successor list to: {[n['id'] for n in self.successor_list]}")

                except (ValueError, IndexError):
                    print(f"Node {self.id}: Invalid SUCCESSOR_LIST format (port/id): {message}")
            else:
                print(f"Node {self.id}: Invalid SUCCESSOR_LIST format (arg count): {message}")

        elif command == "UPDATE_PREDECESSOR_TO":
            # Expecting "UPDATE_PREDECESSOR_TO ip port id"
            args = args_str.split() # Split the arguments string
            if len(args) == 3:
                try:
                    new_pred_ip = args[0]
                    new_pred_port = int(args[1]) # Use args[1]
                    new_pred_id = int(args[2])   # Use args[2]
                    self.predecessor = {"ip": new_pred_ip, "port": new_pred_port, "id": new_pred_id}
                    print(f"Node {self.id} updated predecessor to Node {self.predecessor['id']}")
                    self.last_predecessor_heartbeat = time.time() # Reset heartbeat
                except ValueError:
                    print(f"Node {self.id}: Invalid UPDATE_PREDECESSOR_TO format (port/id): {message}")
            else:
                print(f"Node {self.id}: Invalid UPDATE_PREDECESSOR_TO format (arg count): {message}")

        elif command == "UPDATE_SUCCESSOR_TO":
            # Expecting "UPDATE_SUCCESSOR_TO ip port id"
            args = args_str.split() # Split the arguments string
            if len(args) == 3:
                try:
                    new_succ_ip = args[0]
                    new_succ_port = int(args[1]) # Use args[1]
                    new_succ_id = int(args[2])   # Use args[2]
                    self.successor = {"ip": new_succ_ip, "port": new_succ_port, "id": new_succ_id}
                    if self.successor_list:
                        self.successor_list[0] = self.successor
                    else:
                        self.successor_list = [self.successor]
                    print(f"Node {self.id} updated successor to Node {self.successor['id']}")
                except ValueError:
                    print(f"Node {self.id}: Invalid UPDATE_SUCCESSOR_TO format (port/id): {message}")
            else:
                print(f"Node {self.id}: Invalid UPDATE_SUCCESSOR_TO format (arg count): {message}")

        # --- File/Directory Commands ---

        elif command == "STORE_FILE":
            # Format: STORE_FILE <remote_path> <base64_content>
            parts_store = args_str.split(' ', 1) # Split path from content
            if len(parts_store) == 2:
                remote_path = parts_store[0]
                content_base64 = parts_store[1]
                key_id = hash_function(remote_path)
                print(f"[Trace] Node {self.id} handling STORE_FILE for '{remote_path}' (ID: {key_id})")
                successor = self.chord.find_successor(key_id)

                if successor and successor["id"] == self.id:
                    print(f"Node {self.id} storing primary file: {remote_path}")
                    if self._save_file_content(remote_path, content_base64):
                        self.data_store[remote_path] = {'type': 'file'}
                        print(f"Node {self.id} replicating file {remote_path} to successors...")
                        replica_message = f"REPLICATE_FILE {remote_path} {content_base64}"
                        for s in self.successor_list[1:]: # Replicate to r-1 successors
                            if s['id'] != self.id:
                                self.send_message(s["ip"], s["port"], replica_message)
                    else:
                        print(f"Node {self.id} FAILED to save primary file: {remote_path}")
                elif successor:
                    print(f"Node {self.id} forwarding STORE_FILE for '{remote_path}' to Node {successor['id']}")
                    self.send_message(successor["ip"], successor["port"], message) # Forward original message
                else:
                    print(f"Node {self.id} could not find successor for STORE_FILE {remote_path}")
            else:
                print(f"Node {self.id}: Invalid STORE_FILE format: {message}")


        elif command == "REPLICATE_FILE":
            # Format: REPLICATE_FILE <remote_path> <base64_content>
            parts_repl = args_str.split(' ', 1) # Split path from content
            if len(parts_repl) == 2:
                remote_path = parts_repl[0]
                content_base64 = parts_repl[1]
                print(f"Node {self.id} storing replica file: {remote_path}")
                if self._save_file_content(remote_path, content_base64):
                    self.replica_store[remote_path] = {'type': 'file'}
                else:
                    print(f"Node {self.id} FAILED to save replica file: {remote_path}")
            else:
                print(f"Node {self.id}: Invalid REPLICATE_FILE format: {message}")


        elif command == "MKDIR":
            # Format: MKDIR <remote_path>
            remote_path = args_str
            if remote_path:
                key_id = hash_function(remote_path)
                print(f"[Trace] Node {self.id} handling MKDIR for '{remote_path}' (ID: {key_id})")
                successor = self.chord.find_successor(key_id)

                if successor and successor["id"] == self.id:
                    print(f"Node {self.id} creating primary directory: {remote_path}")
                    if self._create_directory_local(remote_path):
                        self.data_store[remote_path] = {'type': 'directory'}
                        print(f"Node {self.id} replicating directory {remote_path} to successors...")
                        replica_message = f"REPLICATE_MKDIR {remote_path}"
                        for s in self.successor_list[1:]:
                            if s['id'] != self.id:
                                self.send_message(s["ip"], s["port"], replica_message)
                    else:
                        print(f"Node {self.id} FAILED to create primary directory: {remote_path}")
                elif successor:
                    print(f"Node {self.id} forwarding MKDIR for '{remote_path}' to Node {successor['id']}")
                    self.send_message(successor["ip"], successor["port"], message)
                else:
                    print(f"Node {self.id} could not find successor for MKDIR {remote_path}")
            else:
                print(f"Node {self.id}: Invalid MKDIR format: {message}")


        elif command == "REPLICATE_MKDIR":
            # Format: REPLICATE_MKDIR <remote_path>
            remote_path = args_str
            if remote_path:
                print(f"Node {self.id} storing replica directory: {remote_path}")
                if self._create_directory_local(remote_path):
                    self.replica_store[remote_path] = {'type': 'directory'}
                else:
                    print(f"Node {self.id} FAILED to create replica directory: {remote_path}")
            else:
                print(f"Node {self.id}: Invalid REPLICATE_MKDIR format: {message}")


        elif command == "RETRIEVE_FILE":
            # Format: RETRIEVE_FILE <remote_path> <requester_ip> <requester_port>
            args = args_str.split() # Split arguments
            if len(args) == 3:
                try:
                    remote_path = args[0]
                    requester_ip = args[1]
                    requester_port = int(args[2]) # Use args[2]
                    key_id = hash_function(remote_path)
                    print(f"[Trace] Node {self.id} handling RETRIEVE_FILE for '{remote_path}' (ID: {key_id}) from {requester_ip}:{requester_port}")
                    successor = self.chord.find_successor(key_id)

                    if successor and successor["id"] == self.id:
                        content_base64 = self._read_file_content(remote_path)
                        reply_message = None
                        if content_base64:
                            print(f"Node {self.id} found file '{remote_path}'. Sending content back.")
                            reply_message = f"FILE_CONTENT {remote_path} {content_base64}"
                        # Check replica store only if primary failed
                        elif self.replica_store.get(remote_path, {}).get('type') == 'file':
                            content_base64 = self._read_file_content(remote_path) # Try reading again
                            if content_base64:
                                print(f"Node {self.id} found file '{remote_path}' in replica. Sending content back.")
                                reply_message = f"FILE_CONTENT {remote_path} {content_base64}"

                        if reply_message:
                            # Check message size before sending (optional but recommended)
                            if len(reply_message.encode('utf-8')) > 65500:
                                print(f"Node {self.id} ERROR: File content for '{remote_path}' too large for UDP.")
                                reply_message = f"FILE_NOT_FOUND {remote_path} #Error:ContentTooLarge"
                            self.send_message(requester_ip, requester_port, reply_message)
                        else:
                            print(f"Node {self.id} did NOT find file '{remote_path}' locally (primary or replica).")
                            reply_message = f"FILE_NOT_FOUND {remote_path}"
                            self.send_message(requester_ip, requester_port, reply_message)

                    elif successor:
                        print(f"Node {self.id} forwarding RETRIEVE_FILE for '{remote_path}' to Node {successor['id']}")
                        self.send_message(successor["ip"], successor["port"], message) # Forward original message
                    else:
                        print(f"Node {self.id} could not find successor for RETRIEVE_FILE {remote_path}")
                        reply_message = f"FILE_NOT_FOUND {remote_path}"
                        self.send_message(requester_ip, requester_port, reply_message)
                except (ValueError, IndexError):
                    print(f"Node {self.id}: Invalid RETRIEVE_FILE format (port/args): {message}")
            else:
                print(f"Node {self.id}: Invalid RETRIEVE_FILE format (arg count): {message}")


        elif command == "FILE_CONTENT":
            # Format: FILE_CONTENT <remote_path> <base64_content>
            parts_content = args_str.split(' ', 1) # Split path from content
            if len(parts_content) == 2:
                remote_path = parts_content[0]
                content_base64 = parts_content[1]
                if remote_path in self.pending_get_requests:
                    destination_path = self.pending_get_requests.pop(remote_path)
                    print(f"Node {self.id} received content for remote file '{remote_path}'. Saving to '{destination_path}'")
                    try:
                        content_bytes = base64.b64decode(content_base64)
                        # Ensure local destination directory exists
                        dest_dir = os.path.dirname(destination_path)
                        if dest_dir and not os.path.exists(dest_dir):
                            os.makedirs(dest_dir)
                        with open(destination_path, 'wb') as f:
                            f.write(content_bytes)
                        print(f"Successfully saved '{remote_path}' to '{destination_path}'")
                    except Exception as e:
                        print(f"Node {self.id} ERROR saving retrieved file {remote_path} to {destination_path}: {e}")
                else:
                    print(f"Node {self.id} received unexpected FILE_CONTENT for '{remote_path}'. No pending request found.")
            else:
                print(f"Node {self.id}: Invalid FILE_CONTENT format: {message}")


        elif command == "FILE_NOT_FOUND":
            # Format: FILE_NOT_FOUND <remote_path>
            remote_path = args_str
            if remote_path:
                if remote_path in self.pending_get_requests:
                    local_dest = self.pending_get_requests.pop(remote_path)
                    print(f"Lookup result: File '{remote_path}' not found in the Chord network (intended for '{local_dest}').")
                else:
                    # Might receive this if original request timed out locally but eventually finished
                    print(f"Lookup result: File '{remote_path}' not found in the Chord network (no pending request).")
            else:
                print(f"Node {self.id}: Invalid FILE_NOT_FOUND format: {message}")


        elif command == "LISTDIR":
            # Format: LISTDIR <remote_path> <requester_ip> <requester_port>
            args = args_str.split() # Split arguments
            if len(args) == 3:
                try:
                    remote_path = args[0]
                    requester_ip = args[1]
                    requester_port = int(args[2]) # Use args[2]
                    key_id = hash_function(remote_path)
                    print(f"[Trace] Node {self.id} handling LISTDIR for '{remote_path}' (ID: {key_id}) from {requester_ip}:{requester_port}")
                    successor = self.chord.find_successor(key_id)

                    if successor and successor["id"] == self.id:
                        contents = self._list_directory_local(remote_path)
                        reply_message = None
                        if contents is not None: # Found directory, even if empty
                            content_str = " ".join(contents) if contents else "#EMPTY#" # Use placeholder if empty
                            reply_message = f"DIR_CONTENT {remote_path} {content_str}"
                            print(f"Node {self.id} found directory '{remote_path}'. Sending contents back.")
                        else: # Not found or not a directory
                            reply_message = f"DIR_NOT_FOUND {remote_path}"
                            print(f"Node {self.id} did NOT find directory '{remote_path}' locally.")

                        # Check message size before sending
                        if len(reply_message.encode('utf-8')) > 65500:
                            print(f"Node {self.id} ERROR: Directory listing for '{remote_path}' too large for UDP.")
                            reply_message = f"DIR_NOT_FOUND {remote_path} #Error:ListingTooLarge"
                        self.send_message(requester_ip, requester_port, reply_message)

                    elif successor:
                        print(f"Node {self.id} forwarding LISTDIR for '{remote_path}' to Node {successor['id']}")
                        self.send_message(successor["ip"], successor["port"], message)
                    else:
                        print(f"Node {self.id} could not find successor for LISTDIR {remote_path}")
                        reply_message = f"DIR_NOT_FOUND {remote_path}"
                        self.send_message(requester_ip, requester_port, reply_message)
                except (ValueError, IndexError):
                    print(f"Node {self.id}: Invalid LISTDIR format (port/args): {message}")
            else:
                print(f"Node {self.id}: Invalid LISTDIR format (arg count): {message}")


        elif command == "DIR_CONTENT":
            # Format: DIR_CONTENT <remote_path> [content list space separated]
            parts_dir = args_str.split(' ', 1) # Split path from content list
            if len(parts_dir) >= 1: # Can be 1 if directory is empty (#EMPTY#)
                remote_path = parts_dir[0]
                content_str = parts_dir[1] if len(parts_dir) > 1 else ""
                if content_str == "#EMPTY#":
                    dir_contents = []
                else:
                    dir_contents = content_str.split(' ') if content_str else []
                print(f"Directory listing for '{remote_path}': {dir_contents}")
            else:
                print(f"Node {self.id}: Invalid DIR_CONTENT format: {message}")


        elif command == "DIR_NOT_FOUND":
            # Format: DIR_NOT_FOUND <remote_path>
            remote_path = args_str
            if remote_path:
                print(f"Directory '{remote_path}' not found in the Chord network.")
            else:
                print(f"Node {self.id}: Invalid DIR_NOT_FOUND format: {message}")

        # --- Legacy K/V and Ping/Pong ---
        elif command == "STORE": # Legacy K/V
            parts_kv = args_str.split(' ', 1)
            if len(parts_kv) == 2:
                key = parts_kv[0]
                value = parts_kv[1]
                key_id = hash_function(key)
                print(f"[Trace] Node {self.id} handling legacy STORE for key '{key}' (ID: {key_id})")
                successor = self.chord.find_successor(key_id)
                if successor and successor["id"] == self.id:
                    print(f"Node {self.id} storing legacy key-value: {key}: {value}")
                    self.data_store[key] = value # Store as string
                    for s in self.successor_list[1:]:
                        if s['id'] != self.id:
                            self.send_message(s["ip"], s["port"], f"REPLICATE {key} {value}")
                elif successor:
                    print(f"Node {self.id} forwarding legacy STORE for key '{key}' to Node {successor['id']}")
                    self.send_message(successor["ip"], successor["port"], message)
                else:
                    print(f"Node {self.id} could not find successor for legacy STORE {key}")
            else:
                print(f"Node {self.id}: Invalid legacy STORE format: {message}")


        elif command == "REPLICATE": # Legacy K/V
            parts_kv = args_str.split(' ', 1)
            if len(parts_kv) == 2:
                key = parts_kv[0]
                value = parts_kv[1]
                self.replica_store[key] = value # Store as string
                print(f"Node {self.id} stored replicated legacy key-value: {key}: {value}")
            else:
                print(f"Node {self.id}: Invalid legacy REPLICATE format: {message}")


        elif command == "LOOKUP": # Legacy K/V
            key = args_str
            if key:
                key_id = hash_function(key)
                successor = self.chord.find_successor(key_id)
                if successor and successor["id"] == self.id:
                    value = self.data_store.get(key, None)
                    if not isinstance(value, str): # Check if it's not file/dir metadata
                        value = None
                    if value is None:
                        value = self.replica_store.get(key, "NOT_FOUND")
                        if not isinstance(value, str): # Check replica too
                            value = "NOT_FOUND"
                    self.send_message(addr[0], addr[1], f"RESULT {key} {value}")
                elif successor:
                    self.send_message(successor["ip"], successor["port"], message)
                else:
                    self.send_message(addr[0], addr[1], f"RESULT {key} NOT_FOUND #Error:NoSuccessor")
            else:
                print(f"Node {self.id}: Invalid legacy LOOKUP format: {message}")


        elif command == "RESULT": # Legacy K/V
            parts_kv = args_str.split(' ', 1)
            if len(parts_kv) == 2:
                key = parts_kv[0]
                value = parts_kv[1]
                print(f"Legacy lookup result for key '{key}': {value}")
            else:
                print(f"Node {self.id}: Invalid legacy RESULT format: {message}")


        elif command == "PING":
            self.send_message(addr[0], addr[1], "PONG")
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()

        elif command == "PONG":
            # Primarily used by is_node_alive, but update heartbeat if it's from predecessor
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()

        else:
            print(f"Node {self.id} received unknown command: {command} from {addr}")

        # ... (rest of Node class methods) ...

    def upload_file(self, local_filepath, remote_path): # Now takes remote_path
        """Initiates uploading a file to a specific remote path."""
        if not os.path.exists(local_filepath):
            print(f"Error: Local file not found: {local_filepath}")
            return
        if not remote_path.startswith('/'):
            print("Error: Remote path must be absolute (start with /)")
            return

        key_id = hash_function(remote_path)
        print(f"Node {self.id} initiating upload for remote path '{remote_path}' (ID: {key_id}) from local '{local_filepath}'")

        try:
            with open(local_filepath, 'rb') as f:
                content_bytes = f.read()
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')

            message = f"STORE_FILE {remote_path} {content_base64}"
            successor = self.chord.find_successor(key_id)

            if successor:
                print(f"Node {self.id} sending STORE_FILE for '{remote_path}' to responsible Node {successor['id']}")
                self.send_message(self.ip, self.port, message) # Route via self
            else:
                print(f"Error: Could not find successor node for path '{remote_path}' (ID: {key_id})")

        except Exception as e:
            print(f"Error reading or encoding file {local_filepath}: {e}")

    def retrieve_file(self, remote_path, destination_path): # Takes remote_path
        """Initiates retrieving a file from a specific remote path."""
        if not remote_path.startswith('/'):
            print("Error: Remote path must be absolute (start with /)")
            return

        key_id = hash_function(remote_path)
        print(f"Node {self.id} initiating retrieval for remote path '{remote_path}' (ID: {key_id}). Saving to '{destination_path}'")

        # Use remote_path as the key for pending requests
        self.pending_get_requests[remote_path] = destination_path
        message = f"RETRIEVE_FILE {remote_path} {self.ip} {self.port}"
        successor = self.chord.find_successor(key_id)

        if successor:
            print(f"Node {self.id} sending RETRIEVE_FILE for '{remote_path}' to responsible Node {successor['id']}")
            self.send_message(self.ip, self.port, message) # Route via self
        else:
             print(f"Error: Could not find successor node for path '{remote_path}' (ID: {key_id})")
             self.pending_get_requests.pop(remote_path, None)

    def make_directory(self, remote_path):
         """Initiates creating a directory at a specific remote path."""
         if not remote_path.startswith('/'):
            print("Error: Remote path must be absolute (start with /)")
            return

         key_id = hash_function(remote_path)
         print(f"Node {self.id} initiating MKDIR for remote path '{remote_path}' (ID: {key_id})")
         message = f"MKDIR {remote_path}"
         successor = self.chord.find_successor(key_id)

         if successor:
             print(f"Node {self.id} sending MKDIR for '{remote_path}' to responsible Node {successor['id']}")
             self.send_message(self.ip, self.port, message) # Route via self
         else:
              print(f"Error: Could not find successor node for path '{remote_path}' (ID: {key_id})")

    def list_directory(self, remote_path):
        """Initiates listing a directory at a specific remote path."""
        if not remote_path.startswith('/'):
            print("Error: Remote path must be absolute (start with /)")
            return

        key_id = hash_function(remote_path)
        print(f"Node {self.id} initiating LISTDIR for remote path '{remote_path}' (ID: {key_id})")
        message = f"LISTDIR {remote_path} {self.ip} {self.port}" # Include self address for reply
        successor = self.chord.find_successor(key_id)

        if successor:
            print(f"Node {self.id} sending LISTDIR for '{remote_path}' to responsible Node {successor['id']}")
            self.send_message(self.ip, self.port, message) # Route via self
        else:
             print(f"Error: Could not find successor node for path '{remote_path}' (ID: {key_id})")


    # --- Update Leave method ---
    def leave(self):
        print(f"Node {self.id} leaving the network.")
        if self.successor and self.successor["id"] != self.id:
            print(f"Transferring data to successor {self.successor['id']}...")
            # Transfer primary data (files and directories)
            for path, meta in list(self.data_store.items()):
                if meta.get('type') == 'file':
                    content_base64 = self._read_file_content(path)
                    if content_base64:
                        print(f"  Transferring primary file: {path}")
                        self.send_message(self.successor["ip"], self.successor["port"], f"STORE_FILE {path} {content_base64}")
                    else:
                        print(f"  Warning: Could not read primary file {path} for transfer.")
                elif meta.get('type') == 'directory':
                     print(f"  Transferring primary directory: {path}")
                     self.send_message(self.successor["ip"], self.successor["port"], f"MKDIR {path}")
                elif isinstance(meta, str): # Legacy K/V pair
                     self.send_message(self.successor["ip"], self.successor["port"], f"STORE {path} {meta}")


            # Transfer replica data (files and directories)
            for path, meta in list(self.replica_store.items()):
                if meta.get('type') == 'file':
                    content_base64 = self._read_file_content(path)
                    if content_base64:
                        print(f"  Transferring replica file: {path}")
                        self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE_FILE {path} {content_base64}")
                    else:
                         print(f"  Warning: Could not read replica file {path} for transfer.")
                elif meta.get('type') == 'directory':
                     print(f"  Transferring replica directory: {path}")
                     self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE_MKDIR {path}")
                elif isinstance(meta, str): # Legacy K/V pair replica
                     self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE {path} {meta}")

            print("Data transfer initiated.")

            # --- Update Neighbors (Existing) ---
            if self.predecessor:
                self.send_message(self.successor["ip"], self.successor["port"],
                f"UPDATE_PREDECESSOR_TO {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}")
        if self.predecessor and self.predecessor["id"] != self.id:
            self.send_message(self.predecessor["ip"], self.predecessor["port"],
            f"UPDATE_SUCCESSOR_TO {self.successor['ip']} {self.successor['port']} {self.successor['id']}")

        time.sleep(1.5) # Allow slightly more time for messages

        # Reset node state (Existing, plus clear stores)
        print(f"Resetting node {self.id} state...")
        self.predecessor = None
        self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
        self.successor_list = [self.successor]
        self.chord.finger_table = [self.successor] * self.chord.m
        self.data_store.clear()
        self.replica_store.clear()
        self.pending_get_requests.clear()

        print(f"Node {self.id} has left the Chord ring and reset its state.")


    def join(self, known_node_ip, known_node_port): # Make sure join is still correct
        if known_node_ip == self.ip and known_node_port == self.port:
            self.predecessor = None
            self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
            self.successor_list = [self.successor]
            print(f"Node {self.id} initialized as the first node in the ring.")
            # Initialize finger table pointing to self
            self.chord.finger_table = [self.successor] * self.chord.m
        else:
            print(f"Node {self.id} joining ring via {known_node_ip}:{known_node_port}")
            # Reset state before joining
            self.predecessor = None
            self.successor = {"ip": self.ip, "port": self.port, "id": self.id} # Temp successor
            self.successor_list = [self.successor]
            self.chord.finger_table = [self.successor] * self.chord.m # Temp finger table
            # Ask known node to find our actual successor
            self.send_message(known_node_ip, known_node_port, f"FIND_SUCCESSOR {self.id}")
            # Stabilization will eventually fix predecessor and finger table.

    def node_stabilize(self):
        while not self.stop_event.is_set():
            self.chord.prune_successor_list()
            self.chord.stabilize()
            self.chord.update_successor_list()
            # Check if successor is not self before sending NOTIFY
            if self.successor and self.successor['id'] != self.id:
                self.send_message(self.successor["ip"], self.successor["port"], f"NOTIFY {self.id}")
            # Finger table update is handled by fix_fingers
            time.sleep(5) # Or adjust timing

    def fix_fingers(self):
        finger_index = 0
        while not self.stop_event.is_set():
             # Update one finger entry per cycle for less bursty traffic
             start_id = (self.id + 2**finger_index) % (2**self.chord.m)
             successor_info = self.chord.find_successor(start_id)
             if successor_info:
                 self.chord.finger_table[finger_index] = {
                     "ip": successor_info["ip"],
                     "port": successor_info["port"],
                     "id": successor_info["id"]
                 }
             else:
                 # Fallback or keep old entry? If find_successor fails, maybe retry later.
                 # For now, we might keep the old one or set to None/self.
                 # Setting to self successor might be safer if lookup consistently fails.
                 self.chord.finger_table[finger_index] = self.successor

             finger_index = (finger_index + 1) % self.chord.m
             # Adjust sleep time based on desired update frequency and number of fingers (m)
             time.sleep(3) # e.g., update all fingers every m*3 seconds


    def check_predecessor(self):
        while not self.stop_event.is_set():
            if self.predecessor and self.predecessor["id"] != self.id:
                # Use Chord's is_node_alive which includes timeout
                if not self.chord.is_node_alive(self.predecessor, timeout=5): # 5s timeout
                    print(f"Node {self.id} detected failed predecessor {self.predecessor['id']}")
                    self.predecessor = None # Assume predecessor failed
                # No explicit else needed, is_node_alive sends PING implicitly
            time.sleep(7) # Check roughly every 7 seconds

    def store(self, key, value): # This is the OLD K/V store
        print("Using simple Key-Value STORE command.")
        self.send_message(self.ip, self.port, f"STORE {key} {value}")

    def lookup(self, key): # This is the OLD K/V lookup
        print("Using simple Key-Value LOOKUP command.")
        self.send_message(self.ip, self.port, f"LOOKUP {key}")

    # def leave(self):
    #     print(f"Node {self.id} leaving the network.")

    #     # --- Transfer Key-Value Data (Existing) ---
    #     if self.successor and self.successor["id"] != self.id:
    #         print(f"Transferring K/V data to successor {self.successor['id']}...")
    #         for key, value in self.data_store.items():
    #              # Check if it's file metadata (True) or actual K/V data
    #              if isinstance(value, str): # Simple K/V pair
    #                  self.send_message(self.successor["ip"], self.successor["port"], f"STORE {key} {value}")
    #         for key, value in self.replica_store.items():
    #              if isinstance(value, str): # Simple K/V pair replica
    #                  self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE {key} {value}")
    #         print("K/V data transfer initiated.")

    #     # --- NEW: Transfer Files ---
    #     if self.successor and self.successor["id"] != self.id:
    #          print(f"Transferring files to successor {self.successor['id']}...")
    #          # Transfer primary files
    #          for filename in list(self.data_store.keys()): # Iterate over copy of keys
    #              if self.data_store.get(filename) is True: # It's a file marker
    #                  content_base64 = self._read_file_content(filename)
    #                  if content_base64:
    #                      print(f"  Transferring primary file: {filename}")
    #                      self.send_message(self.successor["ip"], self.successor["port"], f"STORE_FILE {filename} {content_base64}")
    #                  else:
    #                      print(f"  Warning: Could not read primary file {filename} for transfer.")

    #          # Transfer replica files
    #          for filename in list(self.replica_store.keys()): # Iterate over copy of keys
    #               if self.replica_store.get(filename) is True: # It's a file marker
    #                  content_base64 = self._read_file_content(filename)
    #                  if content_base64:
    #                      print(f"  Transferring replica file: {filename}")
    #                      self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE_FILE {filename} {content_base64}")
    #                  else:
    #                      print(f"  Warning: Could not read replica file {filename} for transfer.")
    #          print("File transfer initiated.")

    #     # --- Update Neighbors (Existing) ---
    #     if self.successor and self.successor["id"] != self.id and self.predecessor:
    #         self.send_message(self.successor["ip"], self.successor["port"],
    #         f"UPDATE_PREDECESSOR_TO {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}")
    #     if self.predecessor and self.predecessor["id"] != self.id and self.successor:
    #         self.send_message(self.predecessor["ip"], self.predecessor["port"],
    #         f"UPDATE_SUCCESSOR_TO {self.successor['ip']} {self.successor['port']} {self.successor['id']}")

    #     time.sleep(1) # Allow time for messages to be sent

    #     # Reset node state (Existing)
    #     print(f"Resetting node {self.id} state...")
    #     self.predecessor = None
    #     self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
    #     self.successor_list = [self.successor]
    #     self.chord.finger_table = [self.successor] * self.chord.m
    #     self.data_store.clear()
    #     self.replica_store.clear()
    #     self.pending_get_requests.clear()
    #     # Optionally, clean up the local storage directory if desired,
    #     # but usually, we leave it as the node might rejoin.
    #     # import shutil
    #     # if os.path.exists(self.storage_dir):
    #     #     shutil.rmtree(self.storage_dir)

    #     print(f"Node {self.id} has left the Chord ring and reset its state.")
    #     # Don't stop threads or close socket here if using EXIT command from interface

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
