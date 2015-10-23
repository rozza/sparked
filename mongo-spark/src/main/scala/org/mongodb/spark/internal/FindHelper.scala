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

package org.mongodb.spark.internal

import org.mongodb.scala.{ FindObservable, MongoCollection }

import scala.reflect.ClassTag

object FindHelper {

  def getFindObservable[D: ClassTag](collection: MongoCollection[D], findOptions: FindOptions): FindObservable[D] = {
    val findObservable: FindObservable[D] = collection.find[D](findOptions.filter).skip(findOptions.skip).limit(findOptions.limit)

    findOptions.projection.map(_ => findObservable.projection(_))
    findOptions.sort.map(_ => findObservable.sort(_))
    findOptions.maxTime.map(_ => findObservable.maxTime(_))

    findObservable
  }

}
