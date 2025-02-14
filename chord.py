import socket
import time
from utils import in_range

class Chord:
    def __init__(self, node, m=8):
        """
        :param node: The Node instance using this Chord instance.
        :param m: The number of bits in the key (ID) space.
        """
        self.node = node
        self.m = m
        self.finger_table = [None] * m

    def find_successor(self, id):
        """
        Synchronously find the successor node responsible for the given id.
        If the id falls between this node and its current successor, return the successor.
        Otherwise, query the closest preceding node.
        """
        # Special case: only one node in the ring.
        if self.node.id == self.node.successor["id"]:
            return self.node.successor

        # Check if id is in (node.id, successor.id] (inclusive on the end)
        if in_range(id, self.node.id, self.node.successor["id"], include_end=True):
            return self.node.successor
        else:
            candidate = self.closest_preceding_node(id)
            if candidate is None:
                candidate = self.node.successor
            # Use a synchronous RPC call to candidate to continue the lookup.
            succ = self.rpc_find_successor(candidate, id)
            if succ is None:
                # If RPC fails, fall back to the candidate.
                return candidate
            else:
                return succ

    def closest_preceding_node(self, id):
        """
        Return the closest finger preceding the id.
        """
        # Traverse the finger table in reverse order.
        for finger in reversed(self.finger_table):
            if finger and in_range(finger["id"], self.node.id, id):
                return finger
        return None

    def update_finger_table(self):
        """
        Refresh all entries in the finger table.
        For each entry i, compute start = (node.id + 2^i) mod 2^m
        and use find_successor(start) to fill in the finger table.
        """
        for i in range(self.m):
            start = (self.node.id + 2 ** i) % (2 ** self.m)
            succ = self.find_successor(start)
            if succ:
                self.finger_table[i] = {
                    "ip": succ["ip"],
                    "port": succ["port"],
                    "id": succ["id"]
                }
            else:
                self.finger_table[i] = None

    def rpc_find_successor(self, candidate, id):
        """
        Synchronously ask the candidate node for the successor of the given id.
        This method creates a temporary UDP socket, sends a FIND_SUCCESSOR request,
        and waits for the SUCCESSOR reply.
        """
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.settimeout(2)  # Wait up to 2 seconds for a reply.
            # Bind to an ephemeral port.
            temp_sock.bind(('', 0))
            message = f"FIND_SUCCESSOR {id}"
            temp_sock.sendto(message.encode(), (candidate["ip"], candidate["port"]))
            data, _ = temp_sock.recvfrom(1024)
            parts = data.decode().split()
            if parts[0] == "SUCCESSOR":
                succ = {"ip": parts[1], "port": int(parts[2]), "id": int(parts[3])}
                temp_sock.close()
                return succ
        except Exception as e:
            print(f"[Chord.rpc_find_successor] Error contacting candidate {candidate}: {e}")
        return None
    
    def is_node_alive(self, node_info, timeout=1):
        """Synchronous ping to check if a node is alive."""
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.settimeout(timeout)
            temp_sock.bind(('', 0))
            temp_sock.sendto("PING".encode(), (node_info["ip"], node_info["port"]))
            data, _ = temp_sock.recvfrom(1024)
            temp_sock.close()
            return data.decode().strip() == "PONG"
        except Exception as e:
            return False

    def prune_successor_list(self):
        """Remove entries from the successor list that are not responding."""
        alive_list = []
        # Always keep self.node.successor_list[0] (immediate successor) if it's alive.
        for entry in self.node.successor_list:
            # If the entry is self, always keep it.
            if entry["id"] == self.node.id:
                alive_list.append(entry)
            else:
                if self.is_node_alive(entry):
                    alive_list.append(entry)
        # Ensure we have at least one entry (the immediate successor)
        if alive_list:
            self.node.successor_list = alive_list
            self.successor = self.node.successor_list[0]
        else:
            # If no successor is alive, fallback to self.
            self.node.successor_list = [ {"ip": self.node.ip, "port": self.node.port, "id": self.node.id} ]
            self.node.successor = self.node.successor_list[0]

    def stabilize(self):
        if self.node.successor["id"] == self.node.id and self.node.predecessor is not None and self.node.predecessor["id"] != self.node.id:
            self.node.successor = self.node.predecessor
            if self.node.successor_list:
                self.node.successor_list[0] = self.node.successor
            print(f"Node {self.node.id} updated its successor to its predecessor: {self.node.successor}")
        else:
            if self.node.successor["id"] != self.node.id:
                self.node.temp_predecessor = None
                self.node.send_message(self.successor["ip"], self.successor["port"], "GET_PREDECESSOR")
                time.sleep(0.5)
                x = self.node.temp_predecessor
                if x and in_range(x["id"], self.node.id, self.node.successor["id"]):
                    self.node.successor = x
                    if self.node.successor_list:
                        self.node.successor_list[0] = self.node.successor
                    print(f"Node {self.node.id} updated its successor to {self.node.successor} via stabilization")

    def update_successor_list(self):
        if self.node.successor["id"] != self.node.id:
            try:
                temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                temp_sock.settimeout(2)
                temp_sock.bind(('', 0))
                temp_sock.sendto("GET_SUCCESSOR_LIST".encode(), (self.node.successor["ip"], self.node.successor["port"]))
                data, _ = temp_sock.recvfrom(1024)
                parts = data.decode().split()
                if parts[0] == "SUCCESSOR_LIST":
                    new_list = []
                    entries = (len(parts) - 1) // 3
                    for i in range(entries):
                        entry_ip = parts[1 + 3*i]
                        entry_port = int(parts[2 + 3*i])
                        entry_id = int(parts[3 + 3*i])
                        new_list.append({"ip": entry_ip, "port": entry_port, "id": entry_id})
                    self.node.successor_list = [self.node.successor]
                    for entry in new_list:
                        if entry["id"] != self.node.id and len(self.node.successor_list) < self.node.r:
                            self.node.successor_list.append(entry)
                    # print(f"Node {self.node.id} updated its successor list to: {self.node.successor_list}")
                temp_sock.close()
            except Exception as e:
                print(f"[stabilize] Error fetching successor list: {e}")