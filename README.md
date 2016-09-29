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
'''
easy_install 
'''

### Installing Spark


## Usage



## Things to Note

### Unsupported Spark SQL Functions
Currently, pyspark.sql.agg does not support all the aggregate 
functions available in pyspark to be parsed into expressions. In order 
to preserve this package's ability to perform custom combinations of 
aggregations, this means that some aggregate functions are not 
supported. We have not explored every possible function but we are aware
that **countDistinct** is not supported.