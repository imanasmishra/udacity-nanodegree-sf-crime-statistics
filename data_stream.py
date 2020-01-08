import logging
import json
from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", DateType(), True),
    StructField("call_date", DateType(), True),
    StructField("offense_date", DateType(), True),
    StructField("call_time", StringType(), True),
    StructField("call_date_time", TimestampType(), True),
    StructField("disposition", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("common_location", StringType(), True),
])

# define udf
get_window = psf.udf(lambda x: (str(x.start.minute)+' - '+str(x.end.minute)), StringType())
get_hour   = psf.udf(lambda x: x.hour)

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "org-sf-police-department-service-calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 8000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json('value', schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table \
        .select(["crime_id", "original_crime_type_name", "disposition", "call_date", "call_date_time"]) \
    
    # extract hour using udf get_hour
    distinct_table = distinct_table.withColumn("call_hour", get_hour(distinct_table.call_date_time))

    # use call_date_time as watermark of 15 minutes
    distinct_table = distinct_table.withWatermark("call_date_time", "30 minutes ")
    # deduplicate on crime_id with watermark
    distinct_table = distinct_table.dropDuplicates(["call_date_time","crime_id"])
    
    # count the number of crimes by 15 minutes window
    agg_calls_rate_df = distinct_table \
        .groupBy(psf.window(
        	distinct_table.call_date_time, "30 minutes", "30 minutes"),
        	distinct_table.call_date,
        	distinct_table.call_hour,
        	distinct_table.original_crime_type_name,
        	distinct_table.disposition) \
        .count()
    
    # extract 15 minutes time window using udf get_window
    agg_calls_rate_df = agg_calls_rate_df.withColumn("formatted_window", get_window(agg_calls_rate_df.window))

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream

    # write output stream
    #agg_query = agg_calls_rate_df\
    #	.select(["call_date", "call_hour", "formatted_window", "original_crime_type_name","count"])\
    #	.writeStream\
    #	.trigger(processingTime='2 seconds')\
    #	.outputMode('update')\
    #	.format('console')\
    #	.queryName("Crime Rate in Last 15 Minutes")\
    #	.start()

    ## TODO attach a ProgressReporter
    #agg_query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code
    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    radio_code_df = radio_code_df.withColumnRenamed("description", "disposition_description")
    radio_code_df.show()
    
    # TODO join on disposition column
    join_df = agg_calls_rate_df.join(radio_code_df,"disposition")

    join_query = join_df \
    	.select(["call_date", "call_hour", "formatted_window", "original_crime_type_name", "disposition_description", "count"]) \
        .writeStream \
        .trigger(processingTime='30 seconds') \
        .outputMode('update') \
        .format('console') \
        .queryName("Crime Rate in Last 30 Minutes")\
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.sql.shuffle.partitions", 4) \
        .getOrCreate()

    logger.info("Spark started")
    if spark is not None:
        try:
            run_spark_job(spark)
        except:
            logger.info("Exception while running spark job")
        finally:
            spark.stop()
            logger.info("Spark stopped")
