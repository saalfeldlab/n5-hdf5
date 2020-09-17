# n5-hdf5 [![Build Status](https://travis-ci.com/saalfeldlab/n5-hdf5.svg?branch=master)](https://travis-ci.com/saalfeldlab/n5-hdf5)
Best effort N5 implementation on HDF5 files (despite the irony).

Not everything is possible because of the philosophical differences between N5 and HDF5 (and the libraries being used), but most of the naive bread and butter stuff works as expected.

## Works

* nD datasets of primitive types ([u]int8-64,float32,64)
* non-overlapping chunks
* raw and gzip compression
* attributes of primitive types and Strings and arrays of primitive types and strings
* custom attributes that can be encoded as JSON

## Doesn't work

* overlapping chunks
* custom compression
* varlength chunks
* chunks of serialized objects
* multisets or other custom types
* parallel writing (it's HDF5)
