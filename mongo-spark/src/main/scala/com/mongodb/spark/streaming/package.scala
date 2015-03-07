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

package com.mongodb.spark

import com.mongodb.scala.reactivestreams.client.collection._
import com.mongodb.spark.rdd.DocumentRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.bson.BsonValue

import scala.language.implicitConversions

package object streaming {

  implicit def toStreamingContextFunctions(ssc: StreamingContext): StreamingContextFunctions =
    StreamingContextFunctions(ssc)

  implicit def toDocumentRDDFunctions(rdd: RDD[Document]): DocumentRDDFunctions =
    DocumentRDDFunctions(rdd)

  implicit def iterableToDocumentRDDFunctions[D <: Iterable[(String, BsonValue)]](rdd: RDD[D]): DocumentRDDFunctions =
    DocumentRDDFunctions(rdd.map(Document(_)))
}