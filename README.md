# Overview

This project is a building block that implements a simple single-node store system to use as a building block for distributed storage systems.

Check out [Project Wiki](../../wiki) for complete description; here are some of the highlights:

* Key/Value storage with optional opaque metadata (stored along with std metadata), where values can range from tiny to huge: smallest entries inlined in local database, larger offlined to disk
* Last-modified index for building Change List - based synchronization between stores
* Automatic, configurable auto-negotiated on-the-fly compression, decompression (GZIP, LZF, depending on payload size)
* Pluggable DB storage backends: current basic implementation based on [BDB-JE](http://en.wikipedia.org/wiki/BerkeleyDB)

# License

Good old [Apache 2](https://github.com/cowtowncoder/ClusterMate)

# Status

Working as part of a bigger system -- high-level system to be open sources -- but both implementation and API still likely to change before 1.0.

Unit tests exist to verify basic functioning of the single-node data store, when accessed locally (not over network).

More documentation still needed.

# Sub-modules

Project is a multi-module Maven project.
Sub-modules are can be grouped in following categories:

* Low-level utility libraries:
 * `shared`: data structures and utility methods for dealing with things like compression and hash code calculation
* Single-node store implementation
 * `store`: Backend-independent single-node data store implementation that uses a backend implementation
 * `backend-bdb-je`: Backend implementation that uses BDB-JE store (default choice)

More on design on [Wiki](../../wiki).
