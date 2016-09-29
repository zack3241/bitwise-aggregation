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

        self.columns = columns
        self.partition_names = partition_columns

        p_columns = []
        for c in partition_columns:
            p_columns.append(col(c))
        self.partition_columns = p_columns

    def get_bitwise_aggregates(self):
        col_length = len(self.columns)
        max_agg = 2 ** len(self.columns)
        aggregates = []

        for i in range(max_agg):
            aggregate = {}
            for j in range(0, col_length):
                if 2 ** j & i:
                    aggregate[self.columns[j]] = True
                else:
                    aggregate[self.columns[j]] = False

            aggregates.append(deepcopy(aggregate))

        return aggregates

    def get_bitwise_aggregated_df(self, df, aggs):
        if not isinstance(df, DataFrame):
            raise TypeError('%s is %s. df must be an instance of pyspark.sql.DataFrame' % (df, type(df)))
        if not isinstance(aggs, dict):
            raise TypeError('%s is %s. aggs must be an instance of dict' % (aggs, type(aggs)))

        aggregates = self.get_bitwise_aggregates()

        print "%s aggregates to process..." % (len(aggregates))
        df_create = True

        for aggregate in aggregates:
            print "Performing aggregate:"
            print aggregate
            fields = []
            for field in aggregate:
                print field
                if aggregate[field]:
                    fields.append(col(field))
                else:
                    fields.append(lit('ALL').name(field))
            fields += self.partition_columns
            current_aggregate = df.groupBy(fields).agg(aggs)

            if df_create:
                bitwise_aggs = current_aggregate
                df_create = False
            else:
                bitwise_aggs = bitwise_aggs.union(current_aggregate)
        print "aggregates complete"
        return bitwise_aggs

    def bitwise_aggregate_to_dir(self,
                                 df,
                                 aggs,
                                 outdir,
                                 out_format = 'csv',
                                 coalesce_num = 1,
                                 batch_num = None,
                                 filename='bitwiseaggregation_csv_output'
                                 ):

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

        aggregates = self.get_bitwise_aggregates()
        if not batch_num:
            batch_num = len(aggregates)
        df.cache()

        count = 0
        batch_count = 0
        df_overwrite = True
        overwrite = True
        batch_data = None

        print "%s aggregates to process..." % (len(aggregates))
        for aggregate in aggregates:
            print "Performing aggregate:"
            print aggregate
            fields = []
            for field in aggregate:
                print field
                if aggregate[field]:
                    fields.append(col(field))
                else:
                    fields.append(lit('ALL').name(field))
            fields += self.partition_columns
            count += 1
            current_aggregate = df.groupBy(fields).agg(aggs)

            if df_overwrite:
                batch_data = current_aggregate
            else:
                batch_data = batch_data.union(current_aggregate)
            df_overwrite = False

            if count % batch_num == 0:
                if overwrite:
                    if coalesce_num:
                        batch_data.coalesce(coalesce_num)\
                            .write.format(out_format)\
                            .save(os.path.join(outdir,filename)
                                  ,mode='overwrite')
                    else:
                        batch_data\
                            .write.format(out_format)\
                            .save(os.path.join(outdir, filename)
                                , mode='overwrite')
                else:
                    if coalesce_num:
                        batch_data.coalesce(coalesce_num)\
                            .write.format(out_format)\
                            .save(os.path.join(outdir,filename)
                                  ,mode='append')
                    else:
                        batch_data\
                            .write.format(out_format)\
                            .save(os.path.join(outdir, filename)
                                , mode='append')
                overwrite = False
                df_overwrite = True
                batch_count += 1
                batch_data = None

        if batch_data:
            if coalesce_num:
                batch_data.coalesce(coalesce_num)\
                    .write.format(out_format)\
                    .save(os.path.join(outdir, filename)
                          , mode='append')
            else:
                batch_data.coalesce(coalesce_num)\
                    .write.format(out_format)\
                    .save(os.path.join(outdir, filename)
                          , mode='append')
        agg_field_names = []
        for a in aggs:
            agg_field_name = aggs[a] + "_of_" + a
            agg_field_names.append(agg_field_name)

        print "Headers will be:"
        print ",".join(self.columns+self.partition_names+agg_field_names)


def bitwise_aggregate_to_dir(self, df, aggs, outdir, out_format='csv', coalesce_num=1, batch_num=None,
                             filename='bitwiseaggregation_csv_output'):
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

    aggregates = self.get_bitwise_aggregates()
    if not batch_num:
        batch_num = len(aggregates)
    df.cache()

    count = 0
    batch_count = 0
    df_overwrite = True
    overwrite = True
    batch_data = None

    print "%s aggregates to process..." % (len(aggregates))
    for aggregate in aggregates:
        print "Performing aggregate:"
        print aggregate
        fields = []
        for field in aggregate:
            print field
            if aggregate[field]:
                fields.append(col(field))
            else:
                fields.append(lit('ALL').name(field))
        fields += self.partition_columns
        count += 1
        current_aggregate = df.groupBy(fields).agg(aggs)

        if df_overwrite:
            batch_data = current_aggregate
        else:
            batch_data = batch_data.union(current_aggregate)
        df_overwrite = False

        if count % batch_num == 0:
            if overwrite:
                if coalesce_num:
                    batch_data.coalesce(coalesce_num) \
                        .write.format(out_format) \
                        .save(os.path.join(outdir, filename)
                              , mode='overwrite')
                else:
                    batch_data \
                        .write.format(out_format) \
                        .save(os.path.join(outdir, filename)
                              , mode='overwrite')
            else:
                if coalesce_num:
                    batch_data.coalesce(coalesce_num) \
                        .write.format(out_format) \
                        .save(os.path.join(outdir, filename)
                              , mode='append')
                else:
                    batch_data \
                        .write.format(out_format) \
                        .save(os.path.join(outdir, filename)
                              , mode='append')
            overwrite = False
            df_overwrite = True
            batch_count += 1
            batch_data = None

    if batch_data:
        if coalesce_num:
            batch_data.coalesce(coalesce_num) \
                .write.format(out_format) \
                .save(os.path.join(outdir, filename)
                      , mode='append')
        else:
            batch_data.coalesce(coalesce_num) \
                .write.format(out_format) \
                .save(os.path.join(outdir, filename)
                      , mode='append')
    agg_field_names = []
    for a in aggs:
        agg_field_name = aggs[a] + "_of_" + a
        agg_field_names.append(agg_field_name)

    print "Headers will be:"
    print ",".join(self.columns + self.partition_names + agg_field_names)