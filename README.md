# Sparked

A quick and dirty MongoDB and Spark connector.

Currently provides a DStream implementation that allows you to query MongoDB and stream the results into Spark.

```
                Here be dragons

    Depends on the new Reactive MongoDB Scala Driver which is under heavy development.
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

package com.mongodb.example

import com.mongodb.scala.reactivestreams.client.Implicits._
import com.mongodb.scala.reactivestreams.client.collection.Document
import com.mongodb.spark._
import org.apache.spark.{ SparkConf, SparkContext }
import org.bson.{ BsonDocument, BsonString }

object PubNames {
  def main(args: Array[String]) {

    val mongoDBOptions = Map(
      "com.mongodb.spark.uri" -> "mongodb://trusty64",
      "com.mongodb.spark.databaseName" -> "demos",
      "com.mongodb.spark.collectionName" -> "pubs"
    )
    val sparkConf = new SparkConf().setMaster("local").setAppName("CustomReceiver").setAll(mongoDBOptions)

    val sc = new SparkContext(sparkConf)
    val pubNames = sc.fromMongoDB()
      .filter(BsonDocument.parse(
        """{"location": {"$geoWithin": {"$geometry": {"type": "Polygon",
        |"coordinates": [[[-13.0, 48.1], [-13.0, 60.75], [4.55, 60.75], [4.55, 48.1], [-13.0, 48.1]]]}}}}""".stripMargin
      ))
      .skip(100)
      .limit(5000) // All in MongoDB
      .map(_.getOrElse("name", new BsonString("Nameless")).asString().getValue) // Now in Spark
      .countByValue()
      .map((kv: (String, Long)) => Document("name" -> kv._1, "count" -> kv._2.toInt))

    sc.saveToMongoDB("aggregated", pubNames) // Back To MongoDB
  }
}
```

## Example use case (Streaming):

```scala

package com.mongodb.example

import com.mongodb.spark._
import com.mongodb.scala.reactivestreams.client.Implicits._
import com.mongodb.scala.reactivestreams.client.collection.Document
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.bson.{ BsonDocument, BsonString }

object PubNames {
  def main(args: Array[String]) {

    val mongoDBOptions = Map(
      "com.mongodb.spark.uri" -> "mongodb://localhost",
      "com.mongodb.spark.databaseName" -> "demos",
      "com.mongodb.spark.collectionName" -> "pubs"
    )
    val sparkConf = new SparkConf().setMaster("local").setAppName("CustomReceiver").setAll(mongoDBOptions)
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    ssc.fromMongoDB()
      .filter(BsonDocument.parse(
        """{"location": {"$geoWithin": {"$geometry": {"type": "Polygon",
          |"coordinates": [[[-13.0, 48.1], [-13.0, 60.75], [4.55, 60.75], [4.55, 48.1], [-13.0, 48.1]]]}}}}""".stripMargin
      ))
      .skip(100)
      .limit(5000)                                                              // All in MongoDB
      .map(_.getOrElse("name", new BsonString("Nameless")).asString().getValue) // Now in Spark
      .countByValue()
      .transform(rdd => rdd.sortBy(_._2, false))
      .map(rdd => Document("name" -> rdd._1, "count" -> rdd._2.toInt))
      .saveToMongoDB("aggregated")                                              // Back to MongoDB!

    ssc.start()
    ssc.stop(true, true)
    ssc.awaitTermination()
  }
}

```

Run with: `./bin/spark-submit  --class com.mongodb.spark.PubNames  --master "local[8]"  ./mongo/mongoSpark-assembly-0.1-SNAPSHOT.jar`
