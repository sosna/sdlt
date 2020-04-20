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