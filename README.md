# Overview

This project is a building block that implements a simple single-node store system, exposing a basic REST interface as `Servlet`.
Store uses [BDB-JE](http://en.wikipedia.org/wiki/BerkeleyDB) to store entry metadata (and smallest of content entries), and file system for storing larger entries.

Implementation will offer various hooks for customizing things like:

* Entry metadata stored
* Logging/metrics aspects
* Pre- and post-method (PUT/POST, GET, DELETE) hooks

# Status

Experimental

