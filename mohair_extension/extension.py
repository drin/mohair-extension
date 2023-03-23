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

In the naive case, we must compile to new message types such as `SkyRel`, which is a
logical read relation that is used by a Skytether storage service. In a more complex case,
we must prioritize compiling to a new or modified message type instead of a core substrait
message type.
"""


# ------------------------------
# Dependencies

from typing import Any
from dataclasses import dataclass

# >> Ibis

#   |> modules
# NOTE: `stalg` is (probably) short for "substrait algebra"
from ibis.expr import types
from ibis.expr.operations import relations
from ibis_substrait.compiler.translate import stalg

#   |> functions
from ibis_substrait.compiler.translate import translate

#   |> classes
from ibis_substrait.compiler.core import SubstraitCompiler
from mohair_extension.mohair.algebra_pb2 import SkyRel, ExecutionStats

# >> Internal
from mohair_extension.relations import SkyPartition


# ------------------------------
# Classes

class SkyTable(relations.PhysicalTable):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.skyrel = None

    def SetPartition(self, sky_partition):
        self.skyrel = sky_partition
        return self

    def name(self):
        return f'{self.skyrel.domain.key}/{self.skyrel.meta.key}'

    def schema(self):
        return self.skyrel.meta.schema


# ------------------------------
# Functions

@translate.register(SkyTable)
def _translate_mohair( op      : SkyTable
                      ,expr    : types.TableExpr | None = None
                      ,*args   : Any
                      ,compiler: SubstraitCompiler | None = None
                      ,**kwargs: Any                            ) -> stalg.Rel:

    return stalg.Rel(
        extension_leaf=stalg.ExtensionLeafRel(
             common=stalg.RelCommon(direct=stalg.RelCommon.Direct())
            ,detail=SkyRel(
                 domain=op.domain.key
                ,partition=op.meta.key
                ,slices=op.slice_indices()
                ,execstats=ExecutionStats(executed=False)
             )
        )
    )
