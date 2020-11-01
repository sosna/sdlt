# Storing statistical data using Delta Lake

The purpose of this repo is to show how statistical data can be managed using [Delta Lake](https://delta.io).

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

The sample data are based on a simplified version of some of the exchange rates data published by the European Central Bank (ECB). All ECB data can be retrieved using their [REST API](https://sdw-wsrest.ecb.europa.eu).

The data has been simplified by removing unused or uninteresting attributes (uninteresting for the purpose of this demo, that is :)). This has been done to help focusing on the essential, instead of obscuring the screen with unused or uninteresting information. 

For the curious, the full [data structure](https://sdw.ecb.europa.eu/datastructure.do?datasetinstanceid=120) can be seen on the ECB website. The properties that have been kept in the sample files are: FREQ, CURRENCY, CURRENCY_DENOM, EXR_TYPE, EXR_SUFFIX, TIME_PERIOD, OBS_VALUE, OBS_STATUS, COLLECTION, UNIT, UNIT_MULT and DECIMALS.

## Choreography

### Setup

You first need to start the Spark shell, as documented in the instructions provided on the [Delta Lake web site](https://docs.delta.io/latest/quick-start.html#spark-scala-shell). Make sure to start your Spark shell with a reference to the Delta lake libraries (i.e. `--packages io.delta:delta-core_2.11:0.6.1`)

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

The first sample file (`in/data.0.csv`) represents the first load of data. The sample file contains all the monthly data for 2 currencies (NOK and RUB) until December 2019 (**504 data points in total**). CSV files can easily be read in Spark, using something like the following:

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
val check = spark.read.format("delta").load("out/exr")
check.show
check.count
```

### First update: Adding data for January and February 2020

It's now time to add our first update, namely data for January and February 2020 (i.e. 4 new observations). As before, we add a key to the data and perform a quick check.

```scala
val df1 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.1.csv")
val df1k = df1.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df1k.show(false)
df1k.count
```

This time, we use `show(false)`, to avoid truncating values when displaying them.

We should now merge the data. Of course, we know that the file only contains new data points. But typically, you don't know whether the data you receive doesn't also contain updates to existing data, so we'll use the `merge` function, along with both a `whenMatched` and a `whenNotMatched` clauses.

```scala
val exrTable = DeltaTable.forPath(spark, "out/exr/")
exrTable.as("master").
  merge(df1k.as("submission"), "master.key = submission.key").
  whenMatched().updateAll().
  whenNotMatched().insertAll().
  execute()
```

As can be seen, the check is on the `key` property. Of course, we could have checked each dimension individually instead, but using the key makes things more readable... Let's now check the results. The table should contain **508 observations**.

```scala
exrTable.toDF.count
```

### Second update: Adding data for CHF

We will now add CHF data until February 2020 (254 new data points). Technically, this is not different from the first update. However, for statisticians, this represents an extension of the coverage (aka reporting universe), instead of a normal append of new data to existing time series and, therefore, is treated separately. The code below should by now look familiar:

```scala
val df2 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.2.csv")
val df2k = df2.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df2k.show(false)
df2k.count
exrTable.as("master").
  merge(df2k.as("submission"), "master.key = submission.key").
  whenMatched().updateAll().
  whenNotMatched().insertAll().
  execute()
exrTable.toDF.count
```

The latest should return 762.

### Intermezzo: Travelling back in time

As mentioned in the beginning, one of the features of Delta Lake, is that it gives you the possibility to travel back in time. We can do this either by supplying the number of the version we are interested in (i.e. 0 for the first version ever) or a timestamp. Let's use a version number...

```scala
val dfv0 = spark.read.format("delta").option("versionAsOf", 0).load("out/exr")
dfv0.count
```

That should give us 504, i.e. what we had after the initial load.

### Third update: A full replacement

We will now mimic a full replacement, with a twist: All data prior to January 2007 will no longer be included (i.e. 288 data points should be removed, so we should have 474 data points instead of 762).

```scala
val df3 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.3.csv")
val df3k = df3.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df3k.show
df3k.count
```

As we don't need to be particularly subtle in case of full replacement, we can overwrite the existing location and Delta Lake will take care of the rest...

```scala
df3k.write.format("delta").mode("overwrite").save("out/exr")
exrTable.toDF.count
```

As can be seen, although the table has been "overwritten", it's still possible to get back to previous versions of the data. Let's get back the 508 observations we had after the first update...

```scala
val dfv1 = spark.read.format("delta").option("versionAsOf", 1).load("out/exr")
dfv1.count
```

### Fourth update: Sending forecast for March

The fourth update mimics an organization sending forecast data to another one (in this case, forecasts for the 3 currencies, i.e. 3 new data points). For organizations using SDMX for data exchanges (like in this example), forecasts are indicated by setting `OBS_STATUS` to code `F`. The receiver knows that values are likely to be updated. Again, this is an insert, so there is not much differences with previous examples, but, from a business perspective, it's still an interesting use case...

```scala
val df4 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.4.csv")
val df4k = df4.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df4k.show
df4k.count

exrTable.as("master").
  merge(df4k.as("submission"), "master.key = submission.key").
  whenMatched().updateAll().
  whenNotMatched().insertAll().
  execute()

exrTable.toDF.count
```

That should give use 477 data points. You can check the March data as follows. `OBS_STATUS` should be `F` and make sure to check `OBS_VALUE` too.

```scala
val march = exrTable.toDF.
  filter($"TIME_PERIOD" === "2020-03").
  select("CURRENCY", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS")
march.show
```

### Fifth update: Sending final values for March

So far, all the examples we had were inserts, or a delete followed by an insert (i.e. full replacement). It's now time to send the final values for March, i.e. an update to existing data points. For organizations using SDMX for data exchanges (like in this example), normal values are indicated by setting `OBS_STATUS` to code `N`.

```scala
val df5 = spark.read.format("csv").option("header", "true").schema(schema).load("in/data.5.csv")
val df5k = df5.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df5k.show
df5k.count

exrTable.as("master").
  merge(df5k.as("submission"), "master.key = submission.key").
  whenMatched().updateAll().
  whenNotMatched().insertAll().
  execute()

exrTable.toDF.count
```

Of course, the number of data points should be the same, i.e. 477 as the input only contains updates to existing data. Also, we don't really need the `whenNotMatched` clause in this case. However, as previously mentioned, you don't always necessarily know whether the input contains new data points, updates to existing ones, or both. Keeping both `whenMatched` and `whenNotMatched` clauses allow catering for all cases.

We can now check the March data and, compared with the forecast data, OBS_STATUS should now be set to `N` and `OBS_VALUE` for CHF and RUB should be different.

```scala
val march2 = exrTable.toDF.
  filter($"TIME_PERIOD" === "2020-03").
  select("CURRENCY", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS")
march2.show
```

### Sixth update: Deleting RUB

Let's now mimic the handling of a request to delete data, namely all RUB data in this case. This should remove 159 data points, thereby leaving us with 318 data points in the table.

```scala
exrTable.delete("CURRENCY = 'RUB'")
exrTable.toDF.count
```

### Seventh update: Updating an attribute used across data points

Although the data are represented as a data frame in these examples, from a business perspective, some of the attributes are "attached" at a higher level, meaning they must be the same for all data points belonging to the same collection of data points (aka time series or slice). To give an example, let's say that it is recommended to publish CHF exchange rates using 5 decimals. At the moment however, the value is set to 4. Setting the same value for all CHF data can be done as follow:

```scala
exrTable.update(col("CURRENCY") === "CHF", Map("DECIMALS" -> lit(5)))
```

The recommended number of decimals should now be set to 5 for CHF but should still be 4 for NOK.

```scala
exrTable.toDF.filter($"CURRENCY" === "CHF").show
exrTable.toDF.filter($"CURRENCY" === "NOK").show
```

## Reading history

We can now use the history feature to summarize all the changes made to the table.

```sh
scala> val hist = exrTable.history()
scala> hist.select("version", "timestamp", "operation", "operationParameters").show(false)
+-------+-------------------+---------+------------------------------------------------+
|version|timestamp          |operation|operationParameters                             |
+-------+-------------------+---------+------------------------------------------------+
|7      |2020-04-20 14:41:36|UPDATE   |[predicate -> (CURRENCY#1157 = CHF)]            |
|6      |2020-04-20 14:18:25|DELETE   |[predicate -> ["(`CURRENCY` = 'RUB')"]]         |
|5      |2020-04-20 13:58:42|MERGE    |[predicate -> (master.`key` = submission.`key`)]|
|4      |2020-04-20 13:44:34|MERGE    |[predicate -> (master.`key` = submission.`key`)]|
|3      |2020-04-20 12:26:28|WRITE    |[mode -> Overwrite, partitionBy -> []]          |
|2      |2020-04-20 12:11:21|MERGE    |[predicate -> (master.`key` = submission.`key`)]|
|1      |2020-04-20 11:58:28|MERGE    |[predicate -> (master.`key` = submission.`key`)]|
|0      |2020-04-20 10:41:26|WRITE    |[mode -> Overwrite, partitionBy -> []]          |
+-------+-------------------+---------+------------------------------------------------+
```

Using the version number or the timestamp, we can now go back to any previous state of the data or even use it to replace the current state (rollback functionality).

## A note about schema evolution

Schema evolve. In the example we have used so far, the sender might want to start providing comments about particular data points, and a dedicated property, OBS_COM, could be used for this purpose.

By default, Delta Lake will report an error in case the schema changes. But you can turn on schema evolution using the following:

```scala
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", true)
```

Let's now create a new schema, with the new property.

```scala
val schema2 = StructType(
  StructField("FREQ", StringType, false) ::
  StructField("CURRENCY", StringType, false) ::
  StructField("CURRENCY_DENOM", StringType, false) ::
  StructField("EXR_TYPE", StringType, false) ::
  StructField("EXR_SUFFIX", StringType, false) ::
  StructField("TIME_PERIOD", StringType, false) ::
  StructField("OBS_VALUE", DoubleType, false) ::
  StructField("OBS_STATUS", StringType, false) ::
  StructField("OBS_COM", StringType, false) ::
  StructField("COLLECTION", StringType, false) ::
  StructField("DECIMALS", IntegerType, false) ::
  StructField("TITLE", StringType, false) ::
  StructField("UNIT", StringType, false) ::
  StructField("UNIT_MULT", StringType, false) ::
  Nil)
```

And let's use it the read the new data.

```scala
val df6 = spark.read.format("csv").option("header", "true").schema(schema2).load("in/data.6.csv")
val df6k = df6.withColumn("KEY",
  concat(col("FREQ"), lit(":"),
  col("CURRENCY"), lit(":"),
  col("CURRENCY_DENOM"), lit(":"),
  col("EXR_TYPE"), lit(":"),
  col("EXR_SUFFIX"), lit(":"),
  col("TIME_PERIOD")))
df6k.show
df6k.count
```

As can be seen, there is now a comment about one data point. Let's now update the table...

```scala
exrTable.as("master").
  merge(df6k.as("submission"), "master.key = submission.key").
  whenMatched().updateAll().
  whenNotMatched().insertAll().
  execute()
```

The operation should succeed. Let's see what the comment was:

```scala
val comment = exrTable.toDF.
  filter($"TIME_PERIOD" === "2020-03" && $"CURRENCY" === "CHF").
  select("CURRENCY", "TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "OBS_COM")
comment.show
```

That's all it took to change the table schema :-).

## Table compaction

A quick look below `out/exr/` will show that many small parquet files have been created by Delta Lake (more than 300 in my cases). This is quite a lot considering the data that were sent.

The number of files being created can be tweaked using properties such as `spark.delta.merge.repartitionBeforeWrite` and `spark.sql.shuffle.partitions`:

- The default value for `spark.sql.shuffle.partitions` is 200. For smaller data, this is too high, while for large data it can be too small. The logic depends on the data but, usually, a value up to twice the number of partitions is a good starting point.
- In addition, repartitioning the data before writing it to disk will help mitigating the issue of the many small files mentioned above.

Additional information is available in the [Performance Tuning section](https://docs.delta.io/latest/delta-update.html#performance-tuning) of the Delta Lake documentation.

Once in a while, it is also considered good practice to manually compact the table.

```scala
spark.read.
   format("delta").
   load("out/exr").
   repartition(4).
   write.
   option("dataChange", "false").
   format("delta").
   mode("overwrite").
   save("out/exr")
```

This does not yet remove the other files though. To achieve this, you need to use the [vacuum utility function](https://docs.delta.io/latest/delta-utility.html#vacuum).
