# Overview

This project is a building block that implements a simple single-node store system to use as a building block for distributed storage systems.

Check out [Project Wiki](../../wiki) for complete description; here are some of the highlights:

* Key/Value storage with optional opaque metadata (stored along with std metadata), where values can range from tiny to huge: smallest entries inlined in local database, larger offlined to disk
* Last-modified index for building Change List - based synchronization between stores
* Automatic, configurable auto-negotiated on-the-fly compression, decompression (GZIP, LZF, depending on payload size)
* Pluggable DB storage backends. Current implementations include:
    * [BDB-JE](http://en.wikipedia.org/wiki/BerkeleyDB) based: mature, default implementation
    * [LevelDB/Java](https://github.com/dain/leveldb) based: complete (as of 1.0), no known problems, but not as extensively used.
    * Other backends are planned for things like `mdb` (openLDAP / lighting) and `Krati`: let us know if you are interested in contributing other backends!

# License

Good old [Apache 2](http://www.apache.org/licenses/LICENSE-2.0.html)

# Status

`StoreMate` is a mature component, and 1.0 version was recently released.
It is used in production systems via inclusion in `ClusterMate` (see below)

Unit tests exist to verify basic functioning of the single-node data store, when accessed locally (not over network).

More documentation still needed.

# Sub-modules

Project is a multi-module Maven project.
Sub-modules are can be grouped in following categories:

* Low-level utility libraries:
 * `shared`: data structures and utility methods for dealing with things like compression and hash code calculation
* Single-node store implementation
 * `store`: Backend-independent single-node data store implementation that uses a backend implementation
 * Backends:
     * `backend-bdb-je`: Backend implementation that uses BDB-JE store (default choice)
     * `backend-leveldb`: Backend implementation that uses `LevelDB` store (new in 0.9.7)

More on design on [Wiki](../../wiki).

# Used In

* [ClusterMate](https://github.com/cowtowncoder/ClusterMate) uses `StoreMate` as its per-node Storage Layer
