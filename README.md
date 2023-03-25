# Overview

A prototype of using ibis-substrait to compile against a substrait extension


# Code

## Substrait code generation
I attempted to modify imports of substrait specifications (extensions.proto and
type.proto) in the generated protobuf wrappers (python). That worked for the code itself,
but what did not work was finding the definitions in the `DESCRIPTOR` variable. Without
fully understanding where `DESCRIPTOR` is looking (it seems to define some inline binary),
I'm not sure what doesn't work, but the problem is that the `.proto` files for the imports
were not found. To fix this problem, we simply dropped the imports since they did not
actually seem to be required.

To clarify the modified imports, when compiling the substrait extension (or any protobufs
here, I guess), it is necessary to replace:

```python
from substrait
```

with:

```python
from ibis_substrait.proto.substrait.ibis
```

This allowed the source dependencies to resolve correctly.

## Custom ibis table classes

There are 2 table classes that seem to be the best to derive from: `UnboundTable` and
`PhysicalTable`. Both of these classes can be found in `ibis.expr.operations.relations`.

The motivation in deriving from the 2 aforementioned table classes is to allow
`SubstraitCompiler` in the `ibis-substrait` library to translate to custom substrait
relations. In this repo, we use `mohair_extension.extension.SkyTable` to translate to a
`SkyRel` substrait relation, defined in `mohair_extension.mohair.algebra_pb2` (generated
from the `submodules/mohair-proto` submodule). The method to insert this custom
translation is to register a function with `ibis_substrait.compiler.translate.translate`,
which is a singledispatch function. An example of registering this function can be found
in `mohair_extension.extension` in the function `_translate_mohair`.

## Example

To show how all of the above works, the file `toolbox/compile_mohair` is a python script
that:
1. creates test tables using sample data
2. creates a simple ibis query expression (just a projection)
3. translates the query expression to substrait using `ibis-substrait`
4. prints the resulting substrait plan

`mohair_extension/relations.py` defines simple data classes while
`mohair_extension/extension.py` consolidates most code that interacts with
`ibis-framework` and `ibis-substrait`. `toolbox/compile_mohair` brings everything together
into a single example.


# Sample Data

Sample data can be found in `resources/sample-data.tsv`. This sample data is a **very
small** excerpt from E-GEOD-100618.
