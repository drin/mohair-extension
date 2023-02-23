# ------------------------------
# Dependencies

# >> Standard modules
from pathlib import Path

# >> Third-party
import pyarrow
import pandas
import ibis

from ibis_substrait.compiler.core import SubstraitCompiler


# >> Internal
from mohair_extension.relations import ( SkyDomain
                                        ,SkyPartition
                                        ,SkyPartitionMeta
                                        ,SkyPartitionSlice)


# ------------------------------
# Module variables
SAMPLE_FPATH  = Path('resources') / 'sample-data.tsv'
SAMPLE_SCHEMA = pyarrow.schema([
     pyarrow.field('gene_id'   , pyarrow.string())
    ,pyarrow.field('cell_id'   , pyarrow.string())
    ,pyarrow.field('expression', pyarrow.float32())
])


def TableFromTSV(data_fpath: Path = SAMPLE_FPATH) -> pyarrow.Table:
    """
    Convenience function that creates an arrow table from :data_fpath: using hard-coded
    assumptions.
    """

    # read data from the given file; assume 3 columns (see `SAMPLE_SCHEMA`)
    with open(data_fpath) as data_handle:
        col_names   = next(data_handle).split(' ')
        data_by_col = [ [] for _ in col_names ]

        for line in data_handle:
            fields = line.strip().split(' ')

            data_by_col[0].append(fields[0])
            data_by_col[1].append(fields[1])
            data_by_col[2].append(float(fields[2]))

    # construct the table and return it
    return pyarrow.Table.from_arrays(
         [
              pyarrow.array(data_by_col[0], type=pyarrow.string())
             ,pyarrow.array(data_by_col[1], type=pyarrow.string())
             ,pyarrow.array(data_by_col[2], type=pyarrow.float32())
         ]
        ,schema=SAMPLE_SCHEMA
    )

def EncodeMetaKey(key_name: str) -> bytes:
    """ Convenience function to encode a metadata key as utf-8. """
    return key_name.encode('utf-8')

def EncodePartitionCount(pcount: int)  -> bytes:
    """ Convenience function to encode partition count in a `size_t` size. """
    return pcount.to_bytes(8)

def EncodeStripeSize(stripe_size: int) -> bytes:
    """ Convenience function to encode partition count in a `uint8_t` size. """
    return stripe_size.to_bytes(1)


def SetPartitionData(sky_partition: SkyPartition, data_table: pyarrow.Table) -> SkyPartition:
    """
    Convenience function to set the data of :sky_partition:. This will overwrite data
    currently held by :sky_partition: with hard-coded assumptions.
    """

    # clear data in the partition, just to be sure
    sky_partition.slices = []

    # cache some data for use in this function
    partition_key = Path(sky_partition.domain.key) / sky_partition.meta.key

    # Create 4 slices, assuming 20 rows in slices of max chunk size 5
    for ndx, pslice in enumerate(data_table.to_batches(max_chunksize=5)):

        # Each slice is treated as a table for simplicity
        sky_partition.slices.append(
            SkyPartitionSlice(
                 slice_index=ndx
                ,key=f'{partition_key};{ndx}'
                ,data=pyarrow.Table.from_batches([pslice])
            )
        )

    # our partition is 3 columns wide, has 4 slices
    sky_partition.meta.slice_width = 3
    sky_partition.meta.slice_count = ndx + 1

    # set schema first, then update `schema_meta` and schema metadata
    sky_partition.meta.schema = data_table.schema
    sky_partition.meta.WithMetadata({
         EncodeMetaKey('partition_count'): EncodePartitionCount(ndx + 1)
        ,EncodeMetaKey('stripe_size')    : EncodeStripeSize(1)
    })

    return sky_partition


def SimpleQuery(data_table):
    return (
        data_table.group_by(data_table.gene_id)
                  .aggregate(
                        cell_count=data_table.count()
                       ,expr_total=data_table.expression.sum()
                   )
    )

def AggregateJoin(left_table, right_table):
    return (
        left_table.outer_join(right_table, ['gene_id'])
          [
               ibis.coalesce(left_table.gene_id, right_table.gene_id).name('gene_id')
              ,(left_table.cell_count + right_table.cell_count).name('cell_count')
              ,(left_table.expr_total + right_table.expr_total).name('expr_total')
          ]
    )


# ------------------------------
# Main logic

if __name__ == '__main__':
    # initialize domain named 'test' and initialize a partition named 'sample'
    test_domain      = SkyDomain('test')
    sample_partition = test_domain.PartitionFor('sample')

    # initialize a table from sample data, then assign into partition
    sample_expr      = TableFromTSV()
    sample_partition = SetPartitionData(sample_partition, sample_expr)

    # try to use sample_partition with ibis
    sample_catalog = [
        (pslice.key, pslice.data.to_pandas())
        for pslice in sample_partition.slices
    ]

    ibis_conn = ibis.pandas.connect(dict(sample_catalog))

    query = SimpleQuery(ibis_conn.table(sample_catalog[0][0]))
    for tname, tdata in sample_catalog[1:]:
        tquery = SimpleQuery(ibis_conn.table(tname))

        query = AggregateJoin(query, tquery)

    # final projection
    query = query[
         query.gene_id
        ,(query.expr_total / query.cell_count).name('expr_avg')
    ]

    print(query.unbind())

    substrait_compiler = SubstraitCompiler()
    proto_msg = substrait_compiler.compile(query.unbind())

    with open('average-expression.substrait', 'wb') as file_handle:
        file_handle.write(proto_msg.SerializeToString())
