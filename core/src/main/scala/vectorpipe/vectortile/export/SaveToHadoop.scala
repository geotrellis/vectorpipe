/*
 * Copyright 2016 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geotrellis.spark.io.hadoop

import geotrellis.spark.render._
import geotrellis.spark.SpatialKey

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._

import scala.collection.concurrent.TrieMap

object SaveToHadoop {
  /** Saves records from an iterator and returns them unchanged.
    *
    * @param  recs      Key, Value records to be saved
    * @param  keyToUri  A function from K (a key) to Hadoop URI
    * @param  toBytes   A function from record to array of bytes
    * @param  conf      Hadoop Configuration to used to get FileSystem
    */
  def saveIterator[K, V](
    recs: Iterator[(K, V)],
    keyToUri: K => String,
    conf: Configuration
  )(toBytes: (K, V) => Array[Byte]): Iterator[(K, V)] = {
    val fsCache = TrieMap.empty[String, FileSystem]

    for ( row @ (key, data) <- recs ) yield {
      val path = keyToUri(key)
      val uri = new URI(path)
      val fs = fsCache.getOrElseUpdate(
        uri.getScheme,
        FileSystem.get(uri, conf))
      val out = fs.create(new Path(path))
      try { out.write(toBytes(key, data)) }
      finally { out.close() }
      row
    }
  }

  /**
    * Sets up saving to Hadoop, but returns an RDD so that writes can
    * be chained.
    *
    * @param keyToUri A function from K (a key) to a Hadoop URI
    */
  def setup[K](
    dataset: Dataset[(K, Array[Byte])],
    keyToUri: K => String
  )(implicit ev: Encoder[(K, Array[Byte])]): Dataset[(K, Array[Byte])] = {
    import dataset.sparkSession.implicits._
    dataset.mapPartitions { partition =>
      saveIterator(partition, keyToUri, new Configuration){ (k, v) => v }
   }
  }

  /**
    * Sets up saving to Hadoop, but returns an RDD so that writes can
    * be chained.
    *
    * @param  keyToUri  A function from K (a key) to a Hadoop URI
    * @param  toBytes   A function from record to array of bytes
    */
  def setup[K, V](
    dataset: Dataset[(K, V)],
    keyToUri: K => String,
    toBytes: (K, V) => Array[Byte]
  )(implicit ev: Encoder[(K, V)]): Dataset[(K, V)] = {
    import dataset.sparkSession.implicits._
    val conf = dataset.sparkSession.sparkContext.hadoopConfiguration
    dataset.mapPartitions { partition =>
      saveIterator(partition, keyToUri, new Configuration)(toBytes)
    }
  }

  /**
    * Saves to Hadoop FileSystem, returns an count of records saved.
    *
    * @param keyToUri A function from K (a key) to a Hadoop URI
    */
  def apply[K](
    dataset: Dataset[(K, Array[Byte])],
    keyToUri: K => String
  )( implicit ev: Encoder[(K, Array[Byte])]): Long = {
    import dataset.sparkSession.implicits._
    setup(dataset, keyToUri).count
  }

  /**
    * Saves to Hadoop FileSystem, returns an count of records saved.
    *
    * @param  keyToUri  A function from K (a key) to a Hadoop URI
    * @param  toBytes   A function from record to array of bytes
    */
  def apply[K, V](
    dataset: Dataset[(K, V)],
    keyToUri: K => String,
    toBytes: (K, V) => Array[Byte]
  )(implicit ev: Encoder[(K, V)]): Long = {
    import dataset.sparkSession.implicits._
    setup(dataset, keyToUri, toBytes).count
  }
}
