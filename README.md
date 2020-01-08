# Kafka Spark Streaming Optimization

This document outlines the Spark streaming config parameters used for optimizing the SF Crime Statistics project as a part of Udacity nanodegree Data Streaming course.

## Project overview
To develope a real-time data pipeline for analytical and data exploration of SF police department crime data. Data is ingested from a JSON file and real-time events emited from Kafka producer and consumer streaming pipeline built using pyspark Spark structured streaming python library. 

## Resorce configuration
Number of cores - 12,
Memory - 16 GB,
CPU - 2.2 GHz

## Tuning

1. **Number of Kafka topic partitions same as number of CPU cores:** In a Spark Streaming job, Kafka partitions map 1:1 with Spark partitions. So to increase parallelism, Kafka topic is created with 12 partitions. It created 12 Spark executor daemons and can be monitored in *htop* utility.
2. Spark master property: **master(*local(\*)*)**. This will use all available cores in CPU.
3. Spark config property: **config("*spark.sql.shuffle.partitions*", 4)**. This parameter gave the optimal throuput of ~2500 processedRowsPerSecond. For higher value, performance is slow.
4. Spark Streaming Properties: **option("maxOffsetsPerTrigger", 8000)**. By setting this parameter, could process microbatch of 8000 at ~2500 processedRowsPerSecond.

## Output

With above configurations, application could able to process microbatch of **8000** records optimally.
