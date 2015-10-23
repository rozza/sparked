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

package org.mongodb.spark.rdd

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }

import org.apache.spark.SparkContext
import org.mongodb.scala._
import org.mongodb.spark.connection.MongoConnector

case class DocumentFunctions(sc: SparkContext) {

  private val mongoConnector = MongoConnector(sc.getConf)

  def saveToMongoDB(collectionName: String, documents: Iterable[Document]): Unit = {
    val futures = new ListBuffer[Future[Seq[Completed]]]
    mongoConnector.getCollection[Document](collectionName).map(collection =>
      futures += collection.insertMany(documents.toList).toFuture())
    Await.ready(Future.sequence(futures), Duration.Inf)
  }

}
