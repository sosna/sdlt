# Tutorial: Storing statistical data using Delta Lake

The purpose of this project is to show how statistical data can be managed using [Delta Lake](https://delta.io).

## Overview

When exchanging statistical data, organizations typically expect one of the following types of messages:

- **Merge** messages: they may contain new data points as well as updates to previously sent ones;
- **Delete** messages: they contain references to information that needs to be deleted;
- **Full replacement** messages: the data they contain is meant to replace whatever has been sent before by the sender for that statistical domain;

In many cases, the receiver wants to keep the history of what he has received (so-called *vintages*). Therefore, deletions (and full replacements) are meant only as logical deletions, and the receiver should be capable of "travelling back in time" to see how the data were at any point in time.

Delta Lake is an open source Spark library offered by Databricks that, among others, offers time travel / data versioning for data stored in HDFS, Azure Data Lake Storage and Amazon S3. Therefore, if you use one of these 3 options for your Data Lake and have similar requirements to the ones described above, Delta Lake might be an interesting option.

## Prerequisites

You need a Spark Scala shell to run the examples below. Follow the instructions provided on the [Delta Lake web site](https://docs.delta.io/latest/quick-start.html#spark-scala-shell) to set it up.  

## Sample Data

The sample data are based on a simplified version of the exchange rates data offered by the European Central Bank (ECB). All ECB data can be retrieved using their [REST API](https://sdw-wsrest.ecb.europa.eu). The queries used will be provided with each use case described in the next section. 

The data has been simplified by removing unused or uninteresting attributes. This has been done to help focusing on the essential, instead of obscuring the screen with unused or uninteresting information. For the curious, the full [data structure](http://sdw.ecb.int/datastructure.do?datasetinstanceid=120) can be seen on the ECB website. The properties that have been kept in the sample files are: FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX, TIME_PERIOD, OBS_VALUE, OBS_STATUS, COLLECTION, UNIT, UNIT_MULT and DECIMALS.

## Choreography

### Setup

You first need to start the Spark shell, as documented in the instructions provided on the [Delta Lake web site](https://docs.delta.io/latest/quick-start.html#spark-scala-shell).

Once this is done, we need to import required dependencies:

```scala
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import io.delta.tables._
```

We can then create a schema for our data. This step is optional and, if you have metadata-driven processes, this would typically be generated automatically out of your metadata:

```scala
val schema = StructType(
  StructField("FREQ", StringType, false) ::
  StructField("CURRENCY", StringType, false) ::
  StructField("CURRENCY_DENOM", StringType, false) ::
  StructField("EXR_TYPE", StringType, false) ::
  StructField("EXR_SUFFIX", StringType, false) ::
  StructField("TIME_PERIOD", StringType, false) ::
  StructField("OBS_VALUE", DoubleType, false) ::
  StructField("OBS_STATUS", StringType, false) ::
  StructField("COLLECTION", StringType, false) ::
  StructField("DECIMALS", IntegerType, false) ::
  StructField("TITLE", StringType, false) ::
  StructField("UNIT", StringType, false) ::
  StructField("UNIT_MULT", StringType, false) ::
  Nil)
```

## Initial load

The first sample file (`in/data.0.csv`) represents the first load of data. The sample file contains all the data for 2 currencies (NOK and RUB) until December 2019 (**504 data points in total**). CSV files can easily be read in Spark, using something like the following:

```scala
val df0 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.0.csv")
```

We will now add a key for each data point. This is not mandatory but it will help the maintenance operations later on (i.e. merge, delete, etc.). In SDMX, the key is made up of the so-called dimensions (including the special time dimension when applicable), which, for our exchange rate data means: FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX, TIME_PERIOD. This is not a tutorial about SDMX but, in a nutshell, SDMX distinguishes between 3 types of "properties": measures (what we care about), dimensions (properties that, when combined together, allow to uniquely identify a data point) and attributes (properties that don't contribute to the identification but provide additional information). Creating the key can be done as follows (`:` is used as separator):

```scala
val df0k = df0.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
```

We can then peak into the data with commands like:

```scala
df0k.show
df0k.count
```

As this is the initial load, we don't have any delta lake table yet. We can create one using:

```scala
df0k.write.format("delta").mode("overwrite").save("out/exr/")
```

As can be noticed, this is standard Spark code. The only delta lake-related information being the choice of format. Now, just to be on the safe side, let's check that we stored what we expected.

```scala
val check = spark.read.format("delta"). load("out/exr")
check.show
check.count
```
