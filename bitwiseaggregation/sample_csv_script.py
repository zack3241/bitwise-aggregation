# This is a sample script showing how one can use the BitwiseSparkAggregator Class
# Here is a sample cmd for running the script:
# spark-submit --py-files sample_csv_script.py -i data_samples/ign.csv -o data/
from bitwiseaggregation import BitwiseSparkAggregator
from pyspark.sql import SparkSession
import sys
import getopt
import os

def main(args, spark):
    try:
        opts, args = getopt.getopt(args, "hi:o:", ["infile=", "outdir="])
    except getopt.GetoptError:
        print 'sample_csv_script.py -i <infile> -o <outdir>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'sample_csv_script.py -i <infile> -o <outdir>'
            sys.exit()
        elif opt in ("-i", "--infile"):
            inputfile = arg
        elif opt in ("-o", "--outdir"):
            outdir = arg

    df = spark.read.csv(
            inputfile, header=True, mode="DROPMALFORMED"
    )

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
    aggs = {
        "*":"count",
        "score":"mean",
    }

    outdir = os.path.join(os.getcwd(),"data")

    bit_agg = BitwiseSparkAggregator(agg_columns, partition_columns)
    bit_agg.bitwise_aggregate_to_dir(df, aggs, outdir, out_format="csv")

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("bitwise_aggregation_case_study")\
        .getOrCreate()

    main(sys.argv[1:], spark)