---
sidebar_position: 0
---

# Project Structure

The project is structured into 6 areas:

* **Base** - Contains the basic stream implementation with checkpointing system and the base vertice types.
* **Core** - Contains the operators such as join, projection etc, and compute expression compilers.
* **Storage** - The storage solution which handles the B+ tree implementation, persistent storage, LRU cache, etc.
* **Substrait** - Handles query parsing, and creating a substrait execution plan from SQL.
* **AspNetCore** - Web implementation for monitoring of the stream, contains the time series database used for monitoring during debugging.
* **Connectors** - All the different connectors such as SQL Server, MongoDB, etc.