# Kafka Spark Streaming Optimization

This document outlines the Spark streaming config parameters used for optimizing the SF Crime Statistics project as a part of Udacity nanodegree Data Streaming course.

## Project overview
Ingest JSON file and emits events from Kafka producer and setup a Spark streaming pipeline and analytical and data exploration using Structured Spark streaming using pyspark. 

## Resorce configuration
Number of cores - 12
Memory - 16 GB
CPU - 2.2 GHz

## Tuning

1. Kafka Topic Partiotion same as numner of CPU core: In a Spark Streaming job, Kafka partitions map 1:1 with Spark partitions. So to increase Parallelism, Kafka topic is created with 12 partition. It creats 12 Spark executor daemons and can be monitored in *htop* utility.
2. Spark master property: master(local(\*)). This will use 12 core available in CPU.
3. Spark config property: config("*spark.sql.shuffle.partitions*", 4). This parameter gave the optimal throuput of ~2500 processedRowsPerSecond.
4. Spark Streaming Properties: option("maxOffsetsPerTrigger", 8000). By setting this parameter, could process microbatch of 8000 at ~2500 processedRowsPerSecond.
