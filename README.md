# Overview

This project is a building block that implements a simple single-node store system to use as a building block for distributed storage systems.

Check out [Project Wiki](StoreMate/wiki) for complete description; here are some of the highlights:

* Key/Value storage with optional opaque metadata (stored along with std metadata), where values can range from tiny to huge: smallest entries inlined in local database, larger offlined to disk
* Last-modified index for building Change List - based synchronization between stores
* Automatic, configurable auto-negotiated on-the-fly compression, decompression (GZIP, LZF, depending on payload size)
* Pluggable DB storage backends: current basic implementation based on [BDB-JE](http://en.wikipedia.org/wiki/BerkeleyDB)

# Status

Working as part of a bigger system -- high-level system to be open sources -- but both implementation and API still likely to change before 1.0.

Unit tests exist to verify basic functioning of the single-node data store, when accessed locally (not over network).

More documentation sorely needed.

# Sub-modules

Project is a multi-module Maven project.
Sub-modules are can be grouped in following categories:

* Low-level utility libraries:
 * `shared`: data structures and utility methods for dealing with things like compression and hash code calculation
 * `json`: [Jackson](https://github.com/FasterXML/jackson-databind) converters for core datatypes (from shared), used for client-server and server-server communication
* Single-node store implementation
 * `store`: Backend-independent single-node data store implementation that uses a backend implementation
 * `backend-bdb-je`: Backend implementation that uses BDB-JE store (default choice)
* Skeletal Client/Server code
 * `client-base`: components to use for building clients (usually HTTP-based) for accessing data in a StoreMate-based clustered data store
 * `server-base`: component to use for building server components of StoreMate-based clustered data store
