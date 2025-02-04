from utils import hash_function, in_range

class Chord:
    def __init__(self, node, m=8):  # m = number of bits for the ID space
        self.node = node
        self.m = m
        self.finger_table = [None] * m
        self.successor = node
        self.predecessor = None

    def find_successor(self, id):
        if self.node.id == self.successor.id:
            return self.node
        elif in_range(id, self.node.id, self.successor.id) or id == self.successor.id:
            return self.successor
        else:
            closest_node = self.closest_preceding_node(id)
            return closest_node.send_find_successor_request(id)

    def closest_preceding_node(self, id):
        for finger in reversed(self.finger_table):
            if finger and in_range(finger.id, self.node.id, id):
                return finger
        return self.node

    def update_finger_table(self):
        for i in range(self.m):
            start = (self.node.id + 2 ** i) % (2 ** self.m)
            self.finger_table[i] = self.find_successor(start)

    def stabilize(self):
        successor_predecessor = self.successor.predecessor
        if successor_predecessor and in_range(successor_predecessor.id, self.node.id, self.successor.id):
            self.successor = successor_predecessor
        self.successor.notify(self.node)

    def notify(self, node):
        if not self.predecessor or in_range(node.id, self.predecessor.id, self.node.id):
            self.predecessor = node
