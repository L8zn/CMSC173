import socket
import time
from utils import hash_function, in_range

class Chord:
    def __init__(self, node, m=8):
        """
        :param node: The Node instance using this Chord instance.
        :param m: The number of bits in the key (ID) space.
        """
        self.node = node
        self.m = m
        self.finger_table = [None] * m

    def init_finger_table(self):
        """
        Optimized initialization of the finger table.
        1. The first finger is set to the successor of (node.id + 1).
        2. The node's successor is updated based on the first finger.
        3. The node's predecessor is obtained from that successor.
        4. Subsequent finger entries are set recursively to reduce lookup overhead.
        """
        mod = 2 ** self.m
        # 1. Set the first finger entry.
        start = (self.node.id + 1) % mod
        self.finger_table[0] = self.find_successor(start)
        # Update the node's successor.
        self.node.successor = self.finger_table[0]
        
        # 2. Obtain the predecessor from the first finger (assumes the node will update node.temp_predecessor).
        self.node.send_message(self.finger_table[0]["ip"], self.finger_table[0]["port"], "GET_PREDECESSOR")
        time.sleep(0.5)  # Brief pause for a reply.
        if hasattr(self.node, 'temp_predecessor') and self.node.temp_predecessor is not None:
            self.node.predecessor = self.node.temp_predecessor

        # 3. Initialize the rest of the finger table.
        for i in range(self.m - 1):
            next_start = (self.node.id + 2 ** (i + 1)) % mod
            # If next_start falls within [node.id, finger[i].id], reuse the finger entry.
            if self.finger_table[i] and in_range(next_start, self.node.id, self.finger_table[i]["id"], include_end=True):
                self.finger_table[i + 1] = self.finger_table[i]
            else:
                self.finger_table[i + 1] = self.find_successor(next_start)

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

    def stabilize(self):
        """
        A version of stabilization that can be called to help update the node's view.
        (Note: The main stabilization is performed by the Node's own stabilize() method.)
        This routine queries the successor for its predecessor and then notifies the successor.
        """
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.settimeout(2)
            temp_sock.bind(('', 0))
            temp_sock.sendto("GET_PREDECESSOR".encode(), (self.node.successor["ip"], self.node.successor["port"]))
            data, _ = temp_sock.recvfrom(1024)
            parts = data.decode().split()
            if parts[0] == "PREDECESSOR" and parts[1] != "NONE":
                pred = {"ip": parts[1], "port": int(parts[2]), "id": int(parts[3])}
                if in_range(pred["id"], self.node.id, self.node.successor["id"], include_end=False):
                    self.node.successor = pred
            temp_sock.close()
        except Exception as e:
            print(f"[Chord.stabilize] Stabilization RPC error: {e}")
        # Notify the successor.
        self.node.send_message(self.node.successor["ip"], self.node.successor["port"], f"NOTIFY {self.node.id}")

    def notify(self, node_info):
        """
        Called when a remote node notifies this node that it might be its predecessor.
        Update the predecessor pointer if needed.
        """
        if (self.node.predecessor is None or
            in_range(node_info["id"], self.node.predecessor["id"], self.node.id, include_end=False)):
            self.node.predecessor = node_info

    def update_others(self):
        """
        Inform all nodes that should have this node in their finger table
        about the joining node.
        """
        new_node_info = {"ip": self.node.ip, "port": self.node.port, "id": self.node.id}
        for i in range(self.m):
            lookup_id = (self.node.id - 2 ** i) % (2 ** self.m)
            pred = self.find_successor(lookup_id)  # Use find_successor to get the predecessor
            # If the found node is not self, send it an update request.
            if pred["id"] != self.node.id:
                self.node.send_message(
                    pred["ip"],
                    pred["port"],
                    f"UPDATE_FINGER {new_node_info['ip']} {new_node_info['port']} {new_node_info['id']} {i}"
                )

    def update_finger_table_entry(self, new_node_info, i):
        """
        Update the finger table entry at index i if the new node
        should be responsible for that slot. If an update occurs,
        propagate the change to the predecessor.
        """
        current_id = self.node.id
        ft_entry = self.finger_table[i]
        # Check if new_node_info["id"] is in [current_id, ft_entry["id"]) in the ID circle.
        if ft_entry is None or in_range(new_node_info["id"], current_id, ft_entry["id"], include_end=False):
            self.finger_table[i] = new_node_info
            if self.node.predecessor and self.node.predecessor["id"] != new_node_info["id"]:
                self.node.send_message(
                    self.node.predecessor["ip"],
                    self.node.predecessor["port"],
                    f"UPDATE_FINGER {new_node_info['ip']} {new_node_info['port']} {new_node_info['id']} {i}"
                )