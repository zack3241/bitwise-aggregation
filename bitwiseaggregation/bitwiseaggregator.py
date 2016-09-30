'''
author: zack3241
date: 9/27/16
'''

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col
import os
from copy import deepcopy

class BitwiseSparkAggregator:

    def __init__(self, columns, partition_columns):

        # Error Handling for creating an instance of the class
        if isinstance(columns, list):
            for i in columns:
                if not isinstance(i, str):
                    raise TypeError('%s is %s. All elements in columns must be an instance of str' % (i, type(i)))
        else:
            raise TypeError('Columns must be an instance of list')

        if isinstance(partition_columns, list):
            for i in partition_columns:
                if not isinstance(i,str):
                    raise TypeError('%s is %s. All elements in columns must be an instance of str' %(i, type(i)))
        else:
            raise TypeError('Partition columns must be an instance of list')

        # Set the columns to be used in the bitwise aggregation.
        self.columns = columns

        # Set the columns to remain static in aggregation. These should not be in the columns already established.
        self.partition_names = partition_columns

        # Process the static columns into Spark DataFrame Columns
        p_columns = []
        for c in partition_columns:
            p_columns.append(col(c))
        self.partition_columns = p_columns

    def get_bitwise_aggregates(self):
        # This function produces the bitwise aggregates.
        col_length = len(self.columns)

        # Establish the number of aggregates that will be created.
        max_agg = 2 ** len(self.columns)
        aggregates = []

        for i in range(max_agg):
            aggregate = {}
            for j in range(0, col_length):
                # Compare the current aggregate with the bit position of the field.
                # If the bit for the current aggregate number is 1 for the field position, the field is active for that
                # aggregation.
                if 2 ** j & i:
                    aggregate[self.columns[j]] = True
                else:
                    aggregate[self.columns[j]] = False

            aggregates.append(deepcopy(aggregate))

        return aggregates

    def get_bitwise_aggregated_df(self, df, aggs):
        # This function will take in a Spark DataFrame and the aggregations in the form of a dictionary and return
        # a Spark DataFrame with the aggregated data as the contents.

        # Error Handling for the function.
        if not isinstance(df, DataFrame):
            raise TypeError('%s is %s. df must be an instance of pyspark.sql.DataFrame' % (df, type(df)))
        if not isinstance(aggs, dict):
            raise TypeError('%s is %s. aggs must be an instance of dict' % (aggs, type(aggs)))

        # Get the aggregates to run for the instance's bitwise columns
        aggregates = self.get_bitwise_aggregates()
        print "%s aggregates to process..." % (len(aggregates))

        df_create = True

        # Cache the DataFrame in memory
        df.cache()

        for aggregate in aggregates:
            print "Performing aggregate:"
            print aggregate
            fields = []

            # Establish the bitwise fields for the current aggregation.
            for field in aggregate:
                if aggregate[field]:
                    fields.append(col(field))
                else:
                    fields.append(lit('ALL').name(field))

            # Add in partition columns
            fields += self.partition_columns

            current_aggregate = df.groupBy(fields).agg(aggs)

            # For the first aggregation, the DataFrame is created.
            # Subsequent aggregations are unioned into the previous data.
            if df_create:
                bitwise_aggs = current_aggregate
                df_create = False
            else:
                bitwise_aggs = bitwise_aggs.union(current_aggregate)
        print "Aggregation Complete"
        return bitwise_aggs

    def bitwise_aggregate_to_dir(self,
                                 df,
                                 aggs,
                                 outdir,
                                 out_format = 'csv',
                                 coalesce_num = 1,
                                 batch_num = None
                                 ):
        # This function will run the aggregates and then write that data into a file or multiple file's which can be
        # found in the out_dir specified. The coalesce_num parameter will set the number of files to be written per
        # batch. The batch_num parameter will break the number of writes for the aggregation to help ease Spark driver
        # memory issues when working with large amounts of data. The default file format is a csv. You can also specify
        # json or parquet.

        # Error Handling for the function.
        if not isinstance(df, DataFrame):
            raise TypeError('%s is %s. df must be an instance of pyspark.sql.DataFrame' % (df, type(df)))
        if not isinstance(aggs, dict):
            raise TypeError('%s is %s. aggs must be an instance of dict' % (aggs, type(aggs)))
        if not isinstance(outdir, str):
            raise TypeError('%s is %s. outdir must be an instance of str' % (outdir, type(outdir)))
        if not isinstance(out_format, str):
            raise TypeError('%s is %s. out_format must be an instance of str' % (out_format, type(out_format)))
        if not isinstance(coalesce_num, int):
            raise TypeError('%s is %s. coalesce_num must be an instance of int' % (coalesce_num, type(coalesce_num)))
        if not isinstance(batch_num, int) and batch_num:
            raise TypeError('%s is %s. batch_num must be an instance of int' % (batch_num, type(batch_num)))
        if not isinstance(filename, str):
            raise TypeError('%s is %s. filename must be an instance of str' % (filename, type(filename)))

        # Get the aggregates to run for the instance's bitwise columns
        aggregates = self.get_bitwise_aggregates()

        # If batch_num is not provided, there will be only a single batch
        if not batch_num:
            batch_num = len(aggregates)

        # Cache the DataFrame in memory
        df.cache()

        count = 0
        df_overwrite = True
        overwrite = True
        batch_data = None

        print "%s aggregates to process..." % (len(aggregates))
        for aggregate in aggregates:
            print "Performing aggregate:"
            print aggregate
            fields = []
            for field in aggregate:
                # Establish the bitwise fields for the current aggregation.
                if aggregate[field]:
                    fields.append(col(field))
                else:
                    fields.append(lit('ALL').name(field))

            # Add in partition columns
            fields += self.partition_columns
            count += 1

            # Create DataFrame for current aggregation
            current_aggregate = df.groupBy(fields).agg(aggs)

            # For the first aggregation of the batch, the DataFrame is created.
            # Subsequent aggregations are unioned into the previous data.
            if df_overwrite:
                batch_data = current_aggregate
            else:
                batch_data = batch_data.union(current_aggregate)
            df_overwrite = False

            # Test if the batch of aggregates is ready to write
            if count % batch_num == 0:

                # For the first write, the data in the outdir is overwritten.
                # Subsequent writes are appended to the previous writes.
                if overwrite:
                    if coalesce_num:
                        batch_data.coalesce(coalesce_num)\
                            .write.format(out_format)\
                            .save(outdir
                                  ,mode='overwrite')
                    else:
                        batch_data\
                            .write.format(out_format)\
                            .save(outdir
                                 ,mode='overwrite')
                else:
                    if coalesce_num:
                        batch_data.coalesce(coalesce_num)\
                            .write.format(out_format)\
                            .save(outdir
                                  ,mode='append')
                    else:
                        batch_data\
                            .write.format(out_format)\
                            .save(outdir
                                 ,mode='append')

                # Reset flag and DataFrame for the next batch of aggregates
                df_overwrite = True
                batch_data = None

        # If aggregates remain after the batch processing, write the remaining data.
        if batch_data:
            batch_data.coalesce(coalesce_num)\
                .write.format(out_format)\
                .save(outdir
                      ,mode='append')

        # Write out the field names for csv's so that the files aren't polluted with header rows.
        if out_format == "csv":
            agg_field_names = []
            for a in aggs:
                agg_field_name = aggs[a] + "_of_" + a
                agg_field_names.append(agg_field_name)

            print "Fields will be:"
            print ",".join(self.columns+self.partition_names+agg_field_names)