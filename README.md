# Chord Protocol Implementation with Distributed File Storage (Path-Based)

This project implements the Chord protocol, a scalable peer-to-peer lookup service based on the paper by Stoica et al. The implementation includes node joins, finger table construction, stabilization, failure handling (successor lists), and has been extended to support **distributed file and directory storage using hierarchical paths**.

Files or directories can be created/uploaded to any node in the Chord ring using standard absolute paths (e.g., `/users/alice/docs/report.txt`, `/data/images`). The system uses the hash of the **full path** to determine the correct node responsible for storing that specific file or directory entry. Items can then be retrieved from any node using their full path, providing a unified, location-transparent view of the stored namespace.

## Project Structure

-   **`interface.py`**
    Provides a command-line interface (CLI) for interacting with Chord nodes (joining, uploading/retrieving files, creating/listing directories, checking status, etc.).

-   **`node.py`**
    Implements the `Node` class that manages:
    *   UDP communication.
    *   Local file and directory storage in a **hierarchical structure mirroring the remote paths** within a `node_storage_<port>` directory.
    *   Integration with the Chord protocol logic.
    *   Background tasks (listening, stabilization, finger table updates, failure checks).

-   **`chord.py`**
    Contains the core Chord overlay protocol logic:
    *   Finger table updates (`find_successor`, `closest_preceding_node`).
    *   Stabilization (`stabilize`).
    *   Successor list maintenance and pruning (`update_successor_list`, `prune_successor_list`, `is_node_alive`).

-   **`utils.py`**
    Provides utility functions:
    *   SHA-1 based hash function (`hash_function`) for node IDs and key IDs (derived from full paths).
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
*   **Distributed File & Directory Storage:**
    *   Interact using absolute hierarchical paths (e.g., `/path/to/resource`).
    *   Upload files (`UPLOAD`) to specific remote paths.
    *   Create directories (`MKDIR`) at specific remote paths.
    *   Retrieve files (`GET`) from any node using their full path.
    *   List directory contents (`LISTDIR`) - *see limitations below*.
    *   Files/directories are stored on the node responsible for the key derived from `hash(full_path)`.
    *   Nodes maintain the hierarchical structure locally within their storage directory.
*   **Data Replication:** Files and directory entries are replicated to the node's successors (configurable via `r` parameter in `Node`) for basic fault tolerance.
*   **Dynamic Network:** Nodes can join and leave the Chord ring gracefully.
    *   Joining nodes correctly integrate into the ring.
    *   Leaving nodes attempt to transfer their stored files/directories to their successor.
*   **Stabilization & Failure Handling:** Periodic background tasks maintain ring integrity, update finger tables, check for failed predecessors, and prune unresponsive nodes from successor lists.
*   **Command-Line Interface:** Interactive CLI for managing nodes and interacting with the distributed file system.

## Usage (CLI Commands)

Once a node is running via `python interface.py`:

*   **`JOIN <ip> <port>`**: Join the Chord ring by contacting an existing node at `<ip>:<port>`.
*   **`UPLOAD <local_filepath> <remote_filepath>`**: Upload the `<local_filepath>` to the DFS at `<remote_filepath>`. *Remote paths must start with `/`.*
*   **`GET <remote_filepath> <local_destination>`**: Retrieve the file at `<remote_filepath>` from the DFS and save it locally to `<local_destination>`. *Remote paths must start with `/`.*
*   **`MKDIR <remote_path>`**: Create a directory in the DFS at `<remote_path>`. *Remote paths must start with `/`.*
*   **`LISTDIR <remote_path>`**: Request a listing of the contents of the directory at `<remote_path>`. *See limitations below.* *Remote paths must start with `/`.*
*   **`LEAVE`**: Gracefully shut down the node, transferring its data to its successor and notifying neighbors. The process remains running; use `EXIT` to terminate completely.
*   **`INFO`**: Display the current node's ID, IP/Port, Successor, Predecessor, Successor List, Finger Table, and stored path metadata. Useful for debugging.
*   **`EXIT`**: Perform a graceful `LEAVE` and then terminate the node process.

*(Optional: The `STORE <key> <value>` and `LOOKUP <key>` commands for simple key-value storage may still be present if not removed).*

### Example Workflow

1.  **Terminal 1:** `python interface.py` (IP: 127.0.0.1, Port: 5000)
2.  **Terminal 2:** `python interface.py` (IP: 127.0.0.1, Port: 5001) -> `JOIN 127.0.0.1 5000`
3.  **Terminal 3:** `python interface.py` (IP: 127.0.0.1, Port: 5002) -> `JOIN 127.0.0.1 5001`
4.  Wait ~15s for stabilization. Check `INFO` on nodes.
5.  **Terminal 1:** `MKDIR /home`
6.  **Terminal 2:** `MKDIR /home/user`
7.  Create a local file `my_report.txt`.
8.  **Terminal 3:** `UPLOAD my_report.txt /home/user/report_v1.txt` (Observe logs).
9.  **Terminal 1:** `LISTDIR /home/user` (Observe output - may or may not show `report_v1.txt` due to limitations).
10. **Terminal 2:** `GET /home/user/report_v1.txt ./downloaded_report.txt`
11. Check if `./downloaded_report.txt` was created with the correct content.
12. **Terminal 3:** `LEAVE`, then `EXIT`. Observe data transfer in logs.

## Important Considerations & Limitations

*   **`LISTDIR` Limitation:** The `LISTDIR <remote_path>` command only queries the single node responsible for the hash of `<remote_path>`. It returns **only the files/subdirectories physically stored in that specific node's local directory** corresponding to `<remote_path>`. It does **not** aggregate results from other nodes where files within that logical directory might be stored (due to their individual path hashes). Therefore, `LISTDIR` does not provide a complete, filesystem-like view of all logical children of a directory.
*   **UDP File Size Limit:** The current implementation sends entire files (base64 encoded) within single UDP messages. This **will fail** for files larger than the effective UDP datagram size limit (typically ~8KB-64KB). The `DIR_CONTENT` message for `LISTDIR` can also exceed this limit if a directory contains many entries *on the queried node*. This implementation is suitable only for **very small files and directories with few entries per node**.
*   **Reliability:** UDP does not guarantee packet delivery or order. File/directory operations might fail silently without more robust error checking and retransmission logic.
*   **Data Consistency:** While files/directories are replicated, advanced consistency management (e.g., handling concurrent updates, ensuring replicas are fully consistent after failures) is not implemented.
*   **No Dynamic Redistribution:** Files/directories are transferred when a node leaves but are not automatically redistributed if the ring topology changes significantly *after* initial storage.

This implementation serves as an educational tool to demonstrate integrating path-based distributed storage concepts with the Chord protocol. For production use, addressing the limitations (especially file size, reliability, and the `LISTDIR` behavior) would be essential, likely requiring different design choices (e.g., TCP for data, more complex directory management).