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

package com.mongodb.spark.connection

import com.mongodb.scala.reactivestreams.client
import org.apache.spark.SparkConf

import scala.reflect.ClassTag
import scala.util.Try

object MongoConnector {
  def apply(conf: SparkConf): MongoConnector = {
    val uri = conf.get("com.mongodb.spark.uri", "mongodb://")
    val databaseName = conf.get("com.mongodb.spark.databaseName")
    val collectionName = conf.get("com.mongodb.spark.collectionName")
    MongoConnector(uri, databaseName, collectionName)
  }
}

case class MongoConnector(uri: String, databaseName: String, collectionName: String) {

  def getCollection[D: ClassTag](): Try[client.MongoCollection[D]] = {
    Try(client.MongoClient(uri).getDatabase(databaseName).getCollection[D](collectionName))
  }

  def getCollection[D: ClassTag](alternativeCollectionName: String): Try[client.MongoCollection[D]] = {
    Try(client.MongoClient(uri).getDatabase(databaseName).getCollection[D](alternativeCollectionName))
  }
}
