# Overview

A prototype of using ibis-substrait to compile against a substrait extension


# Code generation

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


# Sample Data

Sample data can be found in `resources/sample-data.tsv`. This sample data is a **very
small** excerpt from E-GEOD-100618.
