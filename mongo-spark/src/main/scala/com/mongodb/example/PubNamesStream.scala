/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.example

import com.mongodb.scala.reactivestreams.client.Implicits._
import com.mongodb.scala.reactivestreams.client.collection._
import com.mongodb.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.bson.{ BsonDocument, BsonString }

class PubNamesStream {
  // Using pubnames data from: https://github.com/rozza/pubnames

  val mongoDBOptions = Map(
    "com.mongodb.spark.uri" -> "mongodb://trusty64",
    "com.mongodb.spark.databaseName" -> "demos",
    "com.mongodb.spark.collectionName" -> "pubs"
  )
  val sparkConf = new SparkConf().setMaster("local").setAppName("CustomReceiver").setAll(mongoDBOptions)

  // Stream the results
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.fromMongoDB()
    .filter(BsonDocument.parse(
      """{"location": {"$geoWithin": {"$geometry": {"type": "Polygon",
      |"coordinates": [[[-13.0, 48.1], [-13.0, 60.75], [4.55, 60.75], [4.55, 48.1], [-13.0, 48.1]]]}}}}""".stripMargin
    ))
    .skip(100)
    .limit(5000) // All in MongoDB
    .map(_.getOrElse("name", new BsonString("Nameless")).asString().getValue) // Now in Spark
    .countByValue()
    .transform(rdd => rdd.sortBy(_._2, false))
    .map(rdd => Document("name" -> rdd._1, "count" -> rdd._2.toInt))
    .saveToMongoDB("aggregated") // Back To MongoDB

  ssc.start()
  ssc.stop(true, true)
  ssc.awaitTermination()
}