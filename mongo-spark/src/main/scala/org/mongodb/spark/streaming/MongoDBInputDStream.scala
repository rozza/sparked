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

package org.mongodb.spark.streaming

import scala.annotation.meta.param
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.{ Failure, Success }

import org.bson.conversions.Bson

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.mongodb.scala.{ Observer, Subscription }
import org.mongodb.spark.connection.MongoConnector
import org.mongodb.spark.internal.FindHelper.getFindObservable
import org.mongodb.spark.internal.FindOptions

private[streaming] class MongoDBInputDStream[D: ClassTag](
    @(transient @param) ssc_ :StreamingContext,
    connector:               MongoConnector,
    storageLevel:            StorageLevel
) extends ReceiverInputDStream[D](ssc_) with Logging {

  private var findOptions = FindOptions()

  /**
   * Sets the query filter to apply to the query.
   *
   * [[http://docs.mongodb.org/manual/reference/method/db.collection.find/ Filter]]
   * @param filter the filter, which may be null.
   * @return this
   */
  def filter(filter: Bson): MongoDBInputDStream[D] = {
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
  def limit(limit: Int): MongoDBInputDStream[D] = {
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
  def skip(skip: Int): MongoDBInputDStream[D] = {
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
  def maxTime(duration: Duration): MongoDBInputDStream[D] = {
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
  def projection(projection: Bson): MongoDBInputDStream[D] = {
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
  def sort(sort: Bson): MongoDBInputDStream[D] = {
    findOptions = findOptions.copy(sort = Some(sort))
    this
  }

  override def getReceiver(): Receiver[D] = {
    return new MongoDBReceiver[D](connector, findOptions, storageLevel);
  }
}

private[streaming] class MongoDBReceiver[D: ClassTag](
    mongoConnector: MongoConnector,
    findOptions:    FindOptions, storageLevel: StorageLevel
) extends Receiver[D](storageLevel) with Logging {

  private var subscription: Option[Subscription] = None

  override def onStart(): Unit = {
    mongoConnector.getCollection[D]() match {
      case Success(collection) => {
        getFindObservable(collection, findOptions).subscribe(
          new Observer[D] {
            override def onSubscribe(sub: Subscription): Unit = {
              subscription = Some(sub)
              sub.request(Long.MaxValue)
            }
            override def onNext(doc: D): Unit = store(doc)

            override def onError(throwable: Throwable): Unit = stop("Observable errored", throwable)

            override def onComplete(): Unit = stop("publisher finished")

          }
        )
      }
      case Failure(ex) => stop("Failed to connect to MongoDB", ex)
    }
  }

  override def onStop(): Unit = {
    if (subscription.nonEmpty) {
      subscription.get.unsubscribe();
    }
  }
}

