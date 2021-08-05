# n5-hdf5 [![Build Status](https://github.com/saalfeldlab/n5-hdf5/actions/workflows/build-main.yml/badge.svg)](https://github.com/saalfeldlab/n5-hdf5/actions/workflows/build-main.yml)
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
* conversion of arbitrary strings from `JsonElement` after getAttributes (see below)

## Notes on `getAttributes`

As of version 1.4.0, `N5HDF5Reader` implements `GsonAttributesParser` and has the method `getAttributes(String)`
returning a `HashMap<String, JsonElement>`. This method converts String attributes to `JsonObject`s when they
are valid json, so that arbitrary objects can be parsed.

As a result, it strings that are themselves valid json may not be recoverable from the `JsonElement` in the
output HashMap.

Future work may instead store objects using a compound type rather than as json strings
to avoid this issue.

#### Example

As a trivial failure case, we see that storing the string `{     }` (curly brackets with some spaces between
them) produces a `JsonElement` which has lost number of spaces stored in the string.

```
final N5HDF5Writer h5 = new N5HDF5Writer("/home/john/tmp/tmp.h5", 2);
h5.createGroup("attributes");
h5.setAttribute("attributes", "someJsonString", "{     }");
System.out.println( h5.getAttribute("attributes", "someJsonString", String.class ));
System.out.println( h5.getAttributes("attributes").get("someJsonString").toString());
```
produces the output:
```
{     }
{}
```
