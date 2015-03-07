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

package com.mongodb.spark.internal

import org.apache.spark.Logging
import org.reactivestreams.{ Subscription, Subscriber, Publisher }

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ Promise, Future }
import scala.language.implicitConversions

object PublisherHelper extends Logging {

  def publisherToFuture[T](publisher: Publisher[T]): Future[List[T]] = {
    val promise = Promise[List[T]]()
    class FetchingSubscriber extends Subscriber[T]() {
      val results = new ListBuffer[T]()

      override def onSubscribe(s: Subscription): Unit = {
        s.request(Int.MaxValue)
      }

      override def onError(t: Throwable): Unit = {
        promise.failure(t)
      }

      override def onComplete(): Unit = {
        promise.success(results.toList)
      }

      override def onNext(t: T): Unit = results.append(t)
    }
    publisher.subscribe(new FetchingSubscriber())
    promise.future
  }

}
