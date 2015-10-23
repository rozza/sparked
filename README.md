# Sparked

A quick and dirty MongoDB and Spark connector.

Currently provides a DStream implementation that allows you to query MongoDB and stream the results into Spark.

```
                Here be dragons

    Depends on the new MongoDB Scala Driver which is under heavy development.
    As such change expect change and this to be broken at *any* time.

                               ___, ____--'
                          _,-.'_,-'      (
                       ,-' _.-''....____(
             ,))_     /  ,'\ `'-.     (          /\
     __ ,+..a`  \(_   ) /   \    `'-..(         /  \
     )`-;...,_   \(_ ) /     \  ('''    ;'^^`\ <./\.>
         ,_   )   |( )/   ,./^``_..._  < /^^\ \_.))
        `=;; (    (/_')-- -'^^`      ^^-.`_.-` >-'
        `=\\ (                             _,./
          ,\`(                         )^^^
            ``;         __-'^^\       /
              / _>emj^^^   `\..`-.    ``'.
             / /               / /``'`; /
            / /          ,-=='-`=-'  / /
      ,-=='-`=-.               ,-=='-`=-.
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
```

## Example use case (RDD):

```scala

package org.mongodb.example

import org.apache.spark.{SparkConf, SparkContext}
import org.mongodb.scala.Document
import org.mongodb.spark._

object PubNames {
  def main(args: Array[String]) {

    // Using pubnames data from: https://github.com/rozza/pubnames

    val mongoDBOptions = Map(
      "org.mongodb.spark.uri" -> "mongodb://localhost",
      "org.mongodb.spark.databaseName" -> "demos",
      "org.mongodb.spark.collectionName" -> "pubs"
    )
    val sparkConf = new SparkConf().setMaster("local").setAppName("CustomReceiver").setAll(mongoDBOptions)

    val sc = new SparkContext(sparkConf)
    val pubNames = sc.fromMongoDB()
      .filter(Document(
        """{"location": {"$geoWithin": {"$geometry": {"type": "Polygon",
        |"coordinates": [[[-13.0, 48.1], [-13.0, 60.75], [4.55, 60.75], [4.55, 48.1], [-13.0, 48.1]]]}}}}""".stripMargin
      ))
      .skip(100)
      .limit(5000) // All in MongoDB
      .map(_.getOrElse("name", "Nameless").asString().getValue) // Now in Spark
      .countByValue()
      .map((kv: (String, Long)) => Document("name" -> kv._1, "count" -> kv._2.toInt))

    sc.saveToMongoDB("aggregated", pubNames) // Back To MongoDB
  }
}
```

## Example use case (Streaming):

```scala
package org.mongodb.example

import org.mongodb.scala.Document
import org.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }

class PubNamesStream {
  // Using pubnames data from: https://github.com/rozza/pubnames

  val mongoDBOptions = Map(
    "org.mongodb.spark.uri" -> "mongodb://localhost",
    "org.mongodb.spark.databaseName" -> "demos",
    "org.mongodb.spark.collectionName" -> "pubs"
  )
  val sparkConf = new SparkConf().setMaster("local").setAppName("CustomReceiver").setAll(mongoDBOptions)

  // Stream the results
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.fromMongoDB()
    .filter(Document(
      """{"location": {"$geoWithin": {"$geometry": {"type": "Polygon",
      |"coordinates": [[[-13.0, 48.1], [-13.0, 60.75], [4.55, 60.75], [4.55, 48.1], [-13.0, 48.1]]]}}}}""".stripMargin
    ))
    .skip(100)
    .limit(5000) // All in MongoDB
    .map(_.getOrElse("name", "Nameless").asString().getValue) // Now in Spark
    .countByValue()
    .transform(rdd => rdd.sortBy(_._2, false))
    .map(rdd => Document("name" -> rdd._1, "count" -> rdd._2.toInt))
    .saveToMongoDB("aggregatedStream") // Back To MongoDB

  ssc.start()
  ssc.stop(true, true)
  ssc.awaitTermination()
}

```

Run with: `./bin/spark-submit  --class org.mongodb.spark.PubNames  --master "local[8]"  ./mongo/mongo-spark-alldep.jar`
