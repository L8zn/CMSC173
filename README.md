# Chord Protocol Implementation

This project implements the Chord protocol, a scalable peer-to-peer lookup service as described in [Chord: A Scalable Peer-to-peer Lookup Service for Internet Applications]([https://pdos.csail.mit.edu/papers/chord:sigcomm05/chord_sigcomm05.pdf](https://pdos.csail.mit.edu/papers/chord:sigcomm01/chord_sigcomm.pdf)) by Stoica et al. The implementation includes node joins, finger table construction, stabilization, failure handling, and data replication.

## Project Structure

- **interface.py**  
  Provides a command-line interface (CLI) for interacting with Chord nodes.

- **node.py**  
  Implements the Node class that manages UDP communication, data storage (primary and replica), and integrates with the Chord protocol logic.

- **chord.py**  
  Contains the core overlay protocol logic including finger table updates, stabilization, lookups, and successor list maintenance.

- **utils.py**  
  Provides utility functions such as the SHA-1 based hash function, in-range checks, and debugging routines.

## Setup Instructions

1. **Python Environment**  
   Ensure you have Python 3 installed.

2. **Install Dependencies**  
   This project uses [prompt\_toolkit](https://github.com/prompt-toolkit/python-prompt-toolkit) for the CLI. Install it via pip:
   ```bash
   pip install prompt_toolkit
   
3. **Runninng the Project**
   Launch the CLI interface by running:
   ```bash
   python interface.py
