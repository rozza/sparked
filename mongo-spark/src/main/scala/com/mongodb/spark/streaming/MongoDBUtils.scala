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

package com.mongodb.spark.streaming

import com.mongodb.scala.reactivestreams.client.collection.Document
import com.mongodb.spark.connection.MongoConnector
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import scala.reflect.ClassTag

object MongoDBUtils {

  def createStream(ssc: StreamingContext): MongoDBInputDStream[Document] = {
    return createStream(ssc, MongoConnector(ssc.sparkContext.getConf))
  }

  def createStream(ssc: StreamingContext, connector: MongoConnector): MongoDBInputDStream[Document] = {
    return createStream[Document](ssc, connector);
  }

  def createStream[D: ClassTag](ssc: StreamingContext, connector: MongoConnector,
                                storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER): MongoDBInputDStream[D] = {
    return new MongoDBInputDStream[D](ssc, connector, storageLevel);
  }
}
