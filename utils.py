import hashlib

def hash_function(key, m=8):
    """
    Hashes a key using SHA-1 and returns an m-bit integer.
    """
    return int(hashlib.sha1(key.encode()).hexdigest(), 16) % (2 ** m)

def in_range(x, start, end, include_end=False):
    """
    Determines whether x is in the interval (start, end) in a circular ID space.
    
    :param x: The value to test.
    :param start: The start of the interval.
    :param end: The end of the interval.
    :param include_end: If True, use (start, end] instead of (start, end).
    :return: True if x is in the interval, False otherwise.
    """
    if start < end:
        return start < x <= end if include_end else start < x < end
    else:
        # Wrap-around case: the interval spans the end of the ID space.
        return (x > start or x <= end) if include_end else (x > start or x < end)

def display_finger_table(node):
    """
    Displays the finger table for the given node.
    """
    print("\n" + "=" * 16 + " Finger Table " + "=" * 16)
    print("{:<10} {:<20} {:<15}".format("Start", "Interval", "Successor Node"))
    print("-" * 46)

    m = node.chord.m
    for i in range(m):
        start = (node.id + 2 ** i) % (2 ** m)
        end = (node.id + 2 ** (i + 1)) % (2 ** m)
        successor = node.chord.finger_table[i]
        successor_id = successor["id"] if successor else "None"
        interval = f"[{start}, {end})" if start < end else f"[{start}, {2**m}) U [0, {end})"
        print("{:<10} {:<20} {:<15}".format(start, interval, successor_id))

    print("-" * 46)

def debug_node_info(node):
    """
    Displays debugging information for the node, including its ID, IP/port,
    successor, predecessor, and its finger table.
    """
    print("\n=== Node Information ===")
    print(f"Node ID      : {node.id}")
    print(f"IP Address   : {node.ip}")
    print(f"Port         : {node.port}")
    print(f"Successor ID : {node.successor['id'] if node.successor else 'None'}")
    print(f"Predecessor ID: {node.predecessor['id'] if node.predecessor else 'None'}")
    print("=" * 24)
    display_finger_table(node)
