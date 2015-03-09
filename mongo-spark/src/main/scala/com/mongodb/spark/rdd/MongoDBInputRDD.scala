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

package com.mongodb.spark.rdd

import com.mongodb.scala.reactivestreams.client.collection.Document
import com.mongodb.spark.connection.MongoConnector
import com.mongodb.spark.internal.FindHelper.getFindPublisher
import com.mongodb.spark.internal.FindOptions
import com.mongodb.spark.internal.PublisherHelper.{ publisherToFuture, publisherToIterator }
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.bson.conversions.Bson
import org.reactivestreams.Subscription

import scala.concurrent.duration.Duration
import scala.concurrent.{ Await, Future }
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

object MongoDBInputRDD {
  def apply(sc: SparkContext) = new MongoDBInputRDD[Document](sc)
}

class MongoDBInputRDD[D: ClassTag](sc: SparkContext) extends RDD[D](sc, Nil) with Logging {

  private val mongoConnector = MongoConnector(sc.getConf)

  private var findOptions = FindOptions()
  /**
   * Sets the query filter to apply to the query.
   *
   * [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(filter = filter)
    this
  }

  /**
   * Sets the limit to apply.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.limit/#cursor.limit Limit]]
   * @param limit the limit, which may be null
   * @return this
   */
  def limit(limit: Int): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(limit = limit)
    this
  }

  /**
   * Sets the number of documents to skip.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.skip/#cursor.skip Skip]]
   * @param skip the number of documents to skip
   * @return this
   */
  def skip(skip: Int): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(skip = skip)
    this
  }

  /**
   * Sets the maximum execution time on the server for this operation.
   *
   * [[http://docs.mongodb.org/manual/reference/operator/meta/maxTimeMS/ Max Time]]
   * @param duration the duration
   * @return this
   */
  def maxTime(duration: Duration): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(maxTime = Some(duration))
    this
  }

  /**
   * Sets a document describing the fields to return for all matching documents.
   *
   * [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Projection]]
   * @param projection the project document, which may be null.
   * @return this
   */
  def projection(projection: Bson): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(projection = Some(projection))
    this
  }

  /**
   * Sets the sort criteria to apply to the query.
   *
   * [[http://docs.mongodb.org/manual/reference/method/cursor.sort/ Sort]]
   * @param sort the sort criteria, which may be null.
   * @return this
   */
  def sort(sort: Bson): MongoDBInputRDD[D] = {
    findOptions = findOptions.copy(sort = Some(sort))
    this
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[D] = {
    var subscription: Option[Subscription] = None
    mongoConnector.getCollection[D]() match {
      case Success(collection) => {
        publisherToIterator(getFindPublisher(collection, findOptions))
        val future: Future[List[D]] = publisherToFuture(getFindPublisher(collection, findOptions))
        Await.result(future, Duration.Inf).toIterator
      }
      case Failure(ex) => throw new SparkException("Failed to connect to MongoDB", ex)
    }
  }

  override protected def getPartitions: Array[Partition] = Array(new Partition {
    override def index: Int = 0
  })
}
