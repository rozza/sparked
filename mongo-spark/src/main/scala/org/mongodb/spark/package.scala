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

package org.mongodb

import org.mongodb.scala.Document
import org.mongodb.spark.rdd.DocumentRDDFunctions
import org.mongodb.spark.streaming.{ SparkContextFunctions, DocumentDStreamFunctions, StreamingContextFunctions }
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.bson.BsonValue

import _root_.scala.language.implicitConversions

package object spark {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    SparkContextFunctions(sc)

  implicit def toStreamingContextFunctions(ssc: StreamingContext): StreamingContextFunctions =
    StreamingContextFunctions(ssc)

  implicit def toDocumentDStream(dstream: DStream[Document]): DocumentDStreamFunctions =
    DocumentDStreamFunctions(dstream)

  implicit def toDocumentRDDFunctions(rdd: RDD[Document]): DocumentRDDFunctions =
    DocumentRDDFunctions(rdd)

  implicit def iterableToDocumentDStream(dstream: DStream[Iterable[(String, BsonValue)]]): DocumentDStreamFunctions =
    DocumentDStreamFunctions(dstream.map(Document(_)))

  implicit def iterableToDocumentRDDFunctions[D <: Iterable[(String, BsonValue)]](rdd: RDD[D]): DocumentRDDFunctions =
    DocumentRDDFunctions(rdd.map(Document(_)))

}
