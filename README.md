# bitwise-aggregation

This package is built to allow users to quickly aggregate fact data into
a set of aggregate fact data leveraging Spark's in-memory caching. This 
is useful when dealing with large amounts of data in a large fact table 
or file which compressing to aggregates will speed up data exploration 
or processing. Doing bitwise aggregation allows you to compress data 
down to the relevant dimensions you're interested in without 
compromising on the aggregated dimensions themselves. It does this by 
performing all the possible permutations of aggregation for the 
dimensions provided by using bitwise comparisons to turn fields either 
"on" or "off". Then, by leveraging Spark's ability to cache dataframes, 
these large number of aggregations can be preformed very quickly, much 
faster than traditional means. 

*This strategy should be used when attempting to index the fact data 
across is either too costly, not feasible, or not possible.*

## Non-Python Dependencies
This package requires that [Spark 2.0.0](http://spark.apache.org/docs/latest/index.html) is installed. 


## Installation
To install bitwiseaggregation, cd to the bitwise-aggergation and run 
the install command
'''
sudo python setup.py install
'''

### Installing Spark
Spark 2.0.0 is a required dependency for this package to work. To 
install Spark [follow the instructions here]http://spark.apache.org/docs/latest/index.html. 

## Usage
To use this package simply create an instance of BitwiseSparkAggregator
with the columns you with to aggregate upon (aggs) and the columns you
wish to aggregate upon not using the bitwise switching 
(partition_columns):
'''
agg_columns = [
    "score_phrase",
    "platform",
    "genre",
    "editors_choice"
]
partition_columns = [
    "release_year",
    "release_month"
]
bit_agg = BitwiseSparkAggregator(agg_columns, partition_columns)
'''

You can then run the aggregation on a dataframe to create a dataframe to 
access using pyspark or write the data to an directory as a file:
'''
df = spark.read.csv(
            inputfile, header=True, mode="DROPMALFORMED"
    )
# To aggregate the data into a dataframe
bit_agg_df = bit_agg.get_bitwise_aggregated_df(df, aggs)

# To aggregate the data into a csv
bit_agg.bitwise_aggregate_to_dir(df, aggs, outdir)
'''

You can specify additional parameters when writing the data to a 
file or files such as:
* out_format: This sets the format for the files being written. 
** (Default: "csv")
* coalesce_num: This sets the number of files written out per write.
** (Default: 1)
* batch_num:  This sets the number of aggregates to contain in a write 
for the data.
** (Default: All the Aggregates)

## Things to Note

### Unsupported Spark SQL Functions
Currently, pyspark.sql.agg does not support all the aggregate 
functions available in pyspark to be parsed into expressions. In order 
to preserve this package's ability to perform custom combinations of 
aggregations, this means that some aggregate functions are not 
supported. We have not explored every possible function but we are aware
that **countDistinct** is not supported.