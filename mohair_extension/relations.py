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
Code that defines basic relation types used by mohair.
"""


# ------------------------------
# Dependencies

# >> Standard libs
from dataclasses import dataclass

# >> Arrow
from pyarrow import Schema, Table


# ------------------------------
# Classes

@dataclass
class SkyDomain:
    key: str = 'public'

    def PartitionFor(self, partition_key: str) -> 'SkyPartition':
        return SkyPartition(domain=self, meta=SkyPartitionMeta(key=partition_key))

@dataclass
class SkyPartitionMeta:
    slice_width: int                = 0
    slice_count: int                = 0
    key        : str                = None
    schema     : Schema             = None
    schema_meta: dict[bytes, bytes] = None

    def WithMetadata(self, new_meta: dict[bytes, bytes]) -> 'SkyPartitionMeta':
        """ Convenience method to set metadata and update schema.  """

        # replace metadata
        self.schema_meta = new_meta

        # return schema with new metadata
        return self.schema.with_metadata(self.schema_meta)

@dataclass
class SkyPartitionSlice:
    slice_index: int   = 0
    key        : str   = None
    data       : Table = None

    def num_rows(self)    -> int: return self.data.num_rows
    def num_columns(self) -> int: return self.data.num_columns

@dataclass
class SkyPartition:
    domain: SkyDomain               = None
    meta  : SkyPartitionMeta        = None
    slices: list[SkyPartitionSlice] = None

    def __hash__(self):
        return hash(self.domain.key) + hash(self.meta.key)

    def name(self):
        return f'{self.domain.key}/{self.meta.key}'

    def schema(self):
        return self.meta.schema

    def slice_indices(self) -> list[int]:
        return [
            partition_slice.slice_index
            for partition_slice in self.slices
            if partition_slice is not None
        ]
