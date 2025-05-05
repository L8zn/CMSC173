import socket
import threading
import time
import os # <-- Add this
import base64 # <-- Add this for transferring binary data

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
    def _get_local_path(self, filename):
        """Returns the full path where a given filename should be stored locally."""
        return os.path.join(self.storage_dir, filename)

    # === NEW: Helper method to save file content ===
    def _save_file_content(self, filename, content_base64):
        """Saves decoded base64 content to a local file."""
        local_path = self._get_local_path(filename)
        try:
            content_bytes = base64.b64decode(content_base64)
            with open(local_path, 'wb') as f:
                f.write(content_bytes)
            print(f"Node {self.id} saved file: {local_path}")
            return True
        except Exception as e:
            print(f"Node {self.id} ERROR saving file {filename}: {e}")
            return False

    # === NEW: Helper method to read file content ===
    def _read_file_content(self, filename):
        """Reads local file content and returns it base64 encoded."""
        local_path = self._get_local_path(filename)
        try:
            if os.path.exists(local_path):
                with open(local_path, 'rb') as f:
                    content_bytes = f.read()
                return base64.b64encode(content_bytes).decode('utf-8')
            else:
                return None
        except Exception as e:
            print(f"Node {self.id} ERROR reading file {filename}: {e}")
            return None
    # ==================================================

    def listen(self):
        """Continuously listen for incoming UDP messages and handle them."""
        print(f"Node {self.id} listening on {self.ip}:{self.port}")
        while not self.stop_event.is_set():
            try:
                # Potentially increase buffer size if expecting larger messages
                data, addr = self.sock.recvfrom(65535) # Increased buffer size
                message = data.decode('utf-8') # Specify encoding
                # print(f"Node {self.id} received message from {addr}: {message[:100]}...") # Limit print size
                self.handle_message(message, addr)
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
        elif command == "STORE": # Existing STORE is for key-value strings
             key = parts[1]
             value = parts[2] # Assuming value doesn't have spaces for simplicity
             key_id = hash_function(key)
             print(f"[Trace] Node {self.id} handling STORE for key '{key}' (ID: {key_id})")
             successor = self.chord.find_successor(key_id)
             if successor["id"] == self.id:
                 print(f"Node {self.id} storing key-value: {key}: {value}")
                 self.data_store[key] = value
                 # Replicate simple K/V pairs (modify if needed)
                 for s in self.successor_list[1:]:
                    if s['id'] != self.id:
                       self.send_message(s["ip"], s["port"], f"REPLICATE {key} {value}")
             else:
                 print(f"Node {self.id} forwarding STORE for key '{key}' to Node {successor['id']}")
                 self.send_message(successor["ip"], successor["port"], message)

        elif command == "REPLICATE": # Existing REPLICATE is for key-value strings
             key = parts[1]
             value = parts[2]
             self.replica_store[key] = value
             print(f"Node {self.id} stored replicated key-value: {key}: {value}")


        # === NEW: File Storage/Retrieval Commands ===

        elif command == "STORE_FILE":
            # Format: STORE_FILE <filename> <base64_content>
            if len(parts) < 3: return
            filename = parts[1]
            content_base64 = parts[2]
            key_id = hash_function(filename)
            print(f"[Trace] Node {self.id} handling STORE_FILE for '{filename}' (ID: {key_id})")

            successor = self.chord.find_successor(key_id)
            if successor["id"] == self.id:
                # This node is responsible for the file
                print(f"Node {self.id} storing primary file: {filename}")
                if self._save_file_content(filename, content_base64):
                    self.data_store[filename] = True # Mark as primary owner
                    # Replicate the file to successors
                    print(f"Node {self.id} replicating file {filename} to successors: {[s['id'] for s in self.successor_list[1:]]}")
                    for s in self.successor_list[1:]: # Skip self
                        if s['id'] != self.id:
                            # Important: Send the *full* message again
                            replica_message = f"REPLICATE_FILE {filename} {content_base64}"
                            self.send_message(s["ip"], s["port"], replica_message)
                else:
                    print(f"Node {self.id} FAILED to save primary file: {filename}")
            else:
                # Forward the request to the responsible node
                print(f"Node {self.id} forwarding STORE_FILE for '{filename}' to Node {successor['id']}")
                self.send_message(successor["ip"], successor["port"], message) # Forward original message

        elif command == "REPLICATE_FILE":
            # Format: REPLICATE_FILE <filename> <base64_content>
            if len(parts) < 3: return
            filename = parts[1]
            content_base64 = parts[2]
            print(f"Node {self.id} storing replica file: {filename}")
            if self._save_file_content(filename, content_base64):
                self.replica_store[filename] = True # Mark as replica owner
            else:
                 print(f"Node {self.id} FAILED to save replica file: {filename}")

        elif command == "RETRIEVE_FILE":
             # Format: RETRIEVE_FILE <filename> <requester_ip> <requester_port>
             if len(parts) < 4: return
             filename = parts[1]
             requester_ip = parts[2]
             requester_port = int(parts[3])
             key_id = hash_function(filename)
             print(f"[Trace] Node {self.id} handling RETRIEVE_FILE for '{filename}' (ID: {key_id}) from {requester_ip}:{requester_port}")

             successor = self.chord.find_successor(key_id)

             if successor["id"] == self.id:
                 # This node should have the file (primary or replica)
                 content_base64 = self._read_file_content(filename)
                 if content_base64:
                     print(f"Node {self.id} found file '{filename}'. Sending content back.")
                     reply_message = f"FILE_CONTENT {filename} {content_base64}"
                 else:
                     # Check replica store as fallback (though ideally primary check is enough)
                     if filename in self.replica_store:
                         content_base64 = self._read_file_content(filename)
                         if content_base64:
                             print(f"Node {self.id} found file '{filename}' in replica. Sending content back.")
                             reply_message = f"FILE_CONTENT {filename} {content_base64}"
                         else:
                            print(f"Node {self.id} ERROR reading replica file '{filename}'.")
                            reply_message = f"FILE_NOT_FOUND {filename}"
                     else:
                         print(f"Node {self.id} did NOT find file '{filename}'.")
                         reply_message = f"FILE_NOT_FOUND {filename}"

                 self.send_message(requester_ip, requester_port, reply_message)
             else:
                 # Forward the request
                 print(f"Node {self.id} forwarding RETRIEVE_FILE for '{filename}' to Node {successor['id']}")
                 self.send_message(successor["ip"], successor["port"], message) # Forward original message

        elif command == "FILE_CONTENT":
             # Format: FILE_CONTENT <filename> <base64_content>
             if len(parts) < 3: return
             filename = parts[1]
             content_base64 = parts[2]

             # Retrieve the destination path stored earlier
             if filename in self.pending_get_requests:
                 destination_path = self.pending_get_requests.pop(filename) # Remove after use
                 print(f"Node {self.id} received content for file '{filename}'. Saving to '{destination_path}'")
                 try:
                     content_bytes = base64.b64decode(content_base64)
                     # Ensure directory exists before writing
                     os.makedirs(os.path.dirname(destination_path), exist_ok=True)
                     with open(destination_path, 'wb') as f:
                         f.write(content_bytes)
                     print(f"Successfully saved '{filename}' to '{destination_path}'")
                 except Exception as e:
                     print(f"Node {self.id} ERROR saving retrieved file {filename} to {destination_path}: {e}")
             else:
                 print(f"Node {self.id} received unexpected FILE_CONTENT for '{filename}'. No pending request found.")


        elif command == "FILE_NOT_FOUND":
             # Format: FILE_NOT_FOUND <filename>
             if len(parts) < 2: return
             filename = parts[1]
             # Clean up pending request state
             if filename in self.pending_get_requests:
                 self.pending_get_requests.pop(filename)
             print(f"Lookup result: File '{filename}' not found in the Chord network.")

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
            # ... (existing code) ...
            self.send_message(addr[0], addr[1], "PONG")
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        elif command == "PONG":
            # ... (existing code) ...
            if (self.predecessor and addr[0] == self.predecessor["ip"] and addr[1] == self.predecessor["port"]):
                self.last_predecessor_heartbeat = time.time()
        # ... and so on ...
        elif command == "RESULT": # Existing LOOKUP result (for simple K/V)
            key = parts[1]
            value = parts[2]
            print(f"Lookup result for key '{key}': {value}") # Distinguish from file lookup

        else:
            # Only print if it's not one of the handled commands
            # Avoid printing for potentially large file content messages
            if command not in ["FILE_CONTENT", "STORE_FILE", "REPLICATE_FILE"]:
                 print(f"Node {self.id} received unknown command: {command}")

    def upload_file(self, local_filepath):
        """Initiates the process of uploading a file to the Chord ring."""
        if not os.path.exists(local_filepath):
            print(f"Error: File not found locally: {local_filepath}")
            return

        filename = os.path.basename(local_filepath)
        key_id = hash_function(filename)

        print(f"Node {self.id} initiating upload for '{filename}' (ID: {key_id}) from path '{local_filepath}'")

        try:
            with open(local_filepath, 'rb') as f:
                content_bytes = f.read()
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')

            # Construct the message
            # IMPORTANT: Need to handle potential message size limits of UDP!
            # For this lab, we *assume* files are small enough.
            # Real systems need chunking and reliable transfer (TCP or UDP with ACKs).
            message = f"STORE_FILE {filename} {content_base64}"

            # Find the successor for the file's key
            successor = self.chord.find_successor(key_id)

            if successor:
                print(f"Node {self.id} sending STORE_FILE for '{filename}' to responsible Node {successor['id']} ({successor['ip']}:{successor['port']})")
                # Send the message to the local node first, let handle_message route it.
                # This simplifies logic and uses the existing find_successor path.
                self.send_message(self.ip, self.port, message)
            else:
                print(f"Error: Could not find successor node for file '{filename}' (ID: {key_id})")

        except Exception as e:
            print(f"Error reading or encoding file {local_filepath}: {e}")

    def retrieve_file(self, filename, destination_path):
        """Initiates the process of retrieving a file from the Chord ring."""
        key_id = hash_function(filename)
        print(f"Node {self.id} initiating retrieval for '{filename}' (ID: {key_id}). Saving to '{destination_path}'")

        # Store the destination path for when the content arrives
        self.pending_get_requests[filename] = destination_path

        # Construct the message - include requester info so the holder node can reply directly
        message = f"RETRIEVE_FILE {filename} {self.ip} {self.port}"

        # Find the successor for the file's key
        successor = self.chord.find_successor(key_id)

        if successor:
            print(f"Node {self.id} sending RETRIEVE_FILE for '{filename}' to responsible Node {successor['id']} ({successor['ip']}:{successor['port']})")
            # Send the message to the local node first, let handle_message route it.
            self.send_message(self.ip, self.port, message)
        else:
             print(f"Error: Could not find successor node for file '{filename}' (ID: {key_id})")
             self.pending_get_requests.pop(filename, None) # Clean up state

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

    def leave(self):
        print(f"Node {self.id} leaving the network.")

        # --- Transfer Key-Value Data (Existing) ---
        if self.successor and self.successor["id"] != self.id:
            print(f"Transferring K/V data to successor {self.successor['id']}...")
            for key, value in self.data_store.items():
                 # Check if it's file metadata (True) or actual K/V data
                 if isinstance(value, str): # Simple K/V pair
                     self.send_message(self.successor["ip"], self.successor["port"], f"STORE {key} {value}")
            for key, value in self.replica_store.items():
                 if isinstance(value, str): # Simple K/V pair replica
                     self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE {key} {value}")
            print("K/V data transfer initiated.")

        # --- NEW: Transfer Files ---
        if self.successor and self.successor["id"] != self.id:
             print(f"Transferring files to successor {self.successor['id']}...")
             # Transfer primary files
             for filename in list(self.data_store.keys()): # Iterate over copy of keys
                 if self.data_store.get(filename) is True: # It's a file marker
                     content_base64 = self._read_file_content(filename)
                     if content_base64:
                         print(f"  Transferring primary file: {filename}")
                         self.send_message(self.successor["ip"], self.successor["port"], f"STORE_FILE {filename} {content_base64}")
                     else:
                         print(f"  Warning: Could not read primary file {filename} for transfer.")

             # Transfer replica files
             for filename in list(self.replica_store.keys()): # Iterate over copy of keys
                  if self.replica_store.get(filename) is True: # It's a file marker
                     content_base64 = self._read_file_content(filename)
                     if content_base64:
                         print(f"  Transferring replica file: {filename}")
                         self.send_message(self.successor["ip"], self.successor["port"], f"REPLICATE_FILE {filename} {content_base64}")
                     else:
                         print(f"  Warning: Could not read replica file {filename} for transfer.")
             print("File transfer initiated.")

        # --- Update Neighbors (Existing) ---
        if self.successor and self.successor["id"] != self.id and self.predecessor:
            self.send_message(self.successor["ip"], self.successor["port"],
            f"UPDATE_PREDECESSOR_TO {self.predecessor['ip']} {self.predecessor['port']} {self.predecessor['id']}")
        if self.predecessor and self.predecessor["id"] != self.id and self.successor:
            self.send_message(self.predecessor["ip"], self.predecessor["port"],
            f"UPDATE_SUCCESSOR_TO {self.successor['ip']} {self.successor['port']} {self.successor['id']}")

        time.sleep(1) # Allow time for messages to be sent

        # Reset node state (Existing)
        print(f"Resetting node {self.id} state...")
        self.predecessor = None
        self.successor = {"ip": self.ip, "port": self.port, "id": self.id}
        self.successor_list = [self.successor]
        self.chord.finger_table = [self.successor] * self.chord.m
        self.data_store.clear()
        self.replica_store.clear()
        self.pending_get_requests.clear()
        # Optionally, clean up the local storage directory if desired,
        # but usually, we leave it as the node might rejoin.
        # import shutil
        # if os.path.exists(self.storage_dir):
        #     shutil.rmtree(self.storage_dir)

        print(f"Node {self.id} has left the Chord ring and reset its state.")
        # Don't stop threads or close socket here if using EXIT command from interface

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
