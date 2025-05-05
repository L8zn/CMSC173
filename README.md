# Chord Protocol Implementation with Distributed File Storage

This project implements the Chord protocol, a scalable peer-to-peer lookup service based on the paper by Stoica et al. The implementation includes node joins, finger table construction, stabilization, failure handling (successor lists), and has been extended to support **basic distributed file storage**.

Files can be uploaded to any node in the Chord ring. The system uses the hash of the filename to determine the correct node responsible for storing that file. Files can then be retrieved from any node using just the filename, providing a unified, location-transparent view of the stored files.

## Project Structure

-   **`interface.py`**
    Provides a command-line interface (CLI) for interacting with Chord nodes (joining, uploading/retrieving files, checking status, etc.).

-   **`node.py`**
    Implements the `Node` class that manages:
    *   UDP communication.
    *   Local file storage (primary and replica copies) in a `node_storage_<port>` directory.
    *   Integration with the Chord protocol logic.
    *   Background tasks (listening, stabilization, finger table updates, failure checks).

-   **`chord.py`**
    Contains the core Chord overlay protocol logic:
    *   Finger table updates (`find_successor`, `closest_preceding_node`).
    *   Stabilization (`stabilize`).
    *   Successor list maintenance and pruning (`update_successor_list`, `prune_successor_list`, `is_node_alive`).

-   **`utils.py`**
    Provides utility functions:
    *   SHA-1 based hash function (`hash_function`) for node and key (filename) IDs.
    *   Circular ID space range checks (`in_range`).
    *   Node status display routines (`node_info`, `display_finger_table`).

## Setup Instructions

1.  **Python Environment**
    Ensure you have Python 3 installed.

2.  **Install Dependencies**
    This project uses `prompt_toolkit` for the CLI. Install it via pip:
    ```bash
    pip install prompt_toolkit
    ```

3.  **Running the Project**
    Launch the CLI interface for a node by running:
    ```bash
    python interface.py
    ```
    You will be prompted for the IP address and port number for the node. Run this command in separate terminals for each node you want to start.

## Key Features

*   **Chord Protocol:** Implements core Chord concepts (Consistent Hashing, Successor/Predecessor pointers, Finger Tables for efficient lookups).
*   **Distributed File Storage:**
    *   Upload files from any node.
    *   Retrieve files from any node using just the filename.
    *   Files are stored on the node responsible for the key derived from `hash(filename)`.
*   **Data Replication:** Files (and simple K/V data) are replicated to the node's successors (configurable via `r` parameter in `Node`) for basic fault tolerance.
*   **Dynamic Network:** Nodes can join and leave the Chord ring gracefully.
    *   Joining nodes correctly integrate into the ring.
    *   Leaving nodes attempt to transfer their stored files/data to their successor.
*   **Stabilization & Failure Handling:** Periodic background tasks maintain ring integrity, update finger tables, check for failed predecessors, and prune unresponsive nodes from successor lists.
*   **Command-Line Interface:** Interactive CLI for managing nodes and interacting with the distributed file system.

## Usage (CLI Commands)

Once a node is running via `python interface.py`:

*   **`JOIN <ip> <port>`**: Join the Chord ring by contacting an existing node at `<ip>:<port>`.
*   **`UPLOAD <filepath>`**: Upload the local file specified by `<filepath>` to the distributed file system. The filename itself is used to determine the storage location.
*   **`GET <filename> <destination_path>`**: Retrieve the file named `<filename>` from the DFS and save it locally to `<destination_path>`.
*   **`LEAVE`**: Gracefully shut down the node, transferring its data to its successor and notifying neighbors. The process remains running; use `EXIT` to terminate completely.
*   **`INFO`**: Display the current node's ID, IP/Port, Successor, Predecessor, Successor List, and Finger Table. Useful for debugging and understanding the ring state.
*   **`EXIT`**: Perform a graceful `LEAVE` and then terminate the node process.

*(Optional: The `STORE <key> <value>` and `LOOKUP <key>` commands for simple key-value storage from the previous lab may still be present if not removed).*

### Example Workflow

1.  **Terminal 1:** `python interface.py` (IP: 127.0.0.1, Port: 5000)
2.  **Terminal 2:** `python interface.py` (IP: 127.0.0.1, Port: 5001) -> `JOIN 127.0.0.1 5000`
3.  **Terminal 3:** `python interface.py` (IP: 127.0.0.1, Port: 5002) -> `JOIN 127.0.0.1 5001`
4.  Wait ~15s for stabilization. Check `INFO` on nodes.
5.  Create a local file `my_notes.txt`.
6.  **Terminal 2:** `UPLOAD my_notes.txt` (Observe logs to see where it gets stored).
7.  **Terminal 1:** `GET my_notes.txt ./retrieved_notes.txt`
8.  Check if `./retrieved_notes.txt` was created with the correct content.
9.  **Terminal 3:** `LEAVE`, then `EXIT`. Observe data transfer in logs if Node 3 held data.

## Important Considerations & Limitations

*   **UDP File Size Limit:** The current implementation sends entire files (base64 encoded) within single UDP messages. This **will fail** for files larger than the effective UDP datagram size limit (typically ~8KB-64KB, often less). This implementation is suitable only for **very small files**.
*   **Reliability:** UDP does not guarantee packet delivery or order. File uploads/downloads might fail silently without more robust error checking and potential retransmission logic (which is not implemented).
*   **Data Consistency:** While files are replicated, advanced consistency management (e.g., handling concurrent updates, ensuring replicas are fully consistent after failures) is not implemented.
*   **No Dynamic Redistribution:** Files are transferred when a node leaves but are not automatically redistributed if the ring topology changes significantly *after* initial storage (e.g., due to multiple joins/leaves altering successor responsibilities).

This implementation serves as an educational tool to demonstrate integrating distributed storage concepts with the Chord protocol. For production use, addressing the limitations (especially file size and reliability using TCP or chunking/ACKs over UDP) would be essential.