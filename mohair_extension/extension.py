#!/usr/bin/env python

# ------------------------------
# License

# Copyright 2023 Aldrin Montana
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ------------------------------
# Overview
"""
Code that uses the ibis approach to compile substrait using mohair extensions.

Currently, the naive case is supported where we can compile to new message types such as
`SkyRel`--a logical read relation that is used by a Skytether storage service.

A more complex case is not yet supported, which would be to prioritize compiling to a new
or modified message type instead of a core substrait message type.
"""


# ------------------------------
# Dependencies

# >> Standard libs
import typing
from dataclasses import dataclass

# >> Ibis
from ibis.expr             import types, rules, schema as sch
from ibis.expr.operations  import relations
from ibis.backends.pyarrow import datatypes

# >> Ibis-substrait
from ibis_substrait.compiler.translate import stalg
from ibis_substrait.compiler.translate import translate
from ibis_substrait.compiler.core      import SubstraitCompiler

# >> Google
from google.protobuf import any_pb2

# >> Internal
from mohair_extension.relations          import SkyPartition
from mohair_extension.mohair.algebra_pb2 import SkyRel, ExecutionStats


# ------------------------------
# Classes

# NOTE: deriving from PhysicalTable gives a clean base
# class SkyTable(relations.PhysicalTable):
#     skyrel = rules.instance_of(SkyPartition)
# 
#     # NOTE: these properties/attributes are necessary to use `to_expr()` and interact with
#     # it as any other ibis table
#     @property
#     def name(self):
#         return self.skyrel.name()
# 
#     @property
#     def schema(self):
#         return datatypes.from_pyarrow_schema(self.skyrel.schema())

# NOTE: we extend from UnboundTable since schema and name are so useful anyways
class SkyTable(relations.UnboundTable):
    skyrel = rules.instance_of(SkyPartition)


# ------------------------------
# Functions

# this requires ibis 5.0+
def arrow_schema_to_ibis(arrow_schema):
    return datatypes.from_pyarrow_schema(arrow_schema)

@translate.register(SkyTable)
def _translate_mohair( op      : SkyTable
                      ,expr    : types.TableExpr | None = None
                      ,*args   : typing.Any
                      ,compiler: SubstraitCompiler | None = None
                      ,**kwargs: typing.Any                     ) -> stalg.Rel:

    substrait_rel = stalg.Rel(
        extension_leaf=stalg.ExtensionLeafRel(
             common=stalg.RelCommon(direct=stalg.RelCommon.Direct())
        )
    )

    # NOTE: apparently messages have to be packed into an `any_pb2.Any`
    substrait_rel.extension_leaf.detail.Pack(
        SkyRel(
            domain=op.skyrel.domain.key
           ,partition=op.skyrel.meta.key
           ,slices=op.skyrel.slice_indices()
           ,execstats=ExecutionStats(executed=False)
        )
    )

    return substrait_rel
