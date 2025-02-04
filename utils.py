import hashlib

def hash_function(key, m=8):
    """
    Hashes a key using SHA-1 and returns an m-bit integer.
    """
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** m)

def in_range(id, start, end):
    """
    Checks if id is within the range (start, end) in a circular ID space.
    Handles both clockwise and wrap-around conditions.
    """
    if start < end:
        return start < id < end
    else:
        return id > start or id < end

# Utility for debugging and displaying node information
def debug_node_info(node):
    print(f"Node ID: {node.id}, IP: {node.ip}, Port: {node.port}")
    print(f"Successor ID: {node.successor.id if node.successor else 'None'}")
    print(f"Predecessor ID: {node.predecessor.id if node.predecessor else 'None'}")
    print(f"Finger Table: {[f.id if f else 'None' for f in node.chord.finger_table]}")
