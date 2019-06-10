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

package vectorpipe.vectortile.export

import geotrellis.spark.SpatialKey

import cats.effect.{IO, Timer}
import cats.syntax.apply._
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest, PutObjectResult}
import com.amazonaws.services.s3.model.AmazonS3Exception
import org.apache.spark.sql.Dataset

import scala.concurrent.ExecutionContext

import java.io.ByteArrayInputStream
import java.util.concurrent.Executors
import java.net.URI

object SaveToS3 {
  final val defaultThreadCount = Runtime.getRuntime.availableProcessors

  private def defaultConfiguration = {
    val config = new com.amazonaws.ClientConfiguration
    config.setMaxConnections(128)
    config.setMaxErrorRetry(16)
    config.setConnectionTimeout(100000)
    config.setSocketTimeout(100000)
    config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
    config
  }

  /**
    * @param keyToUri  A function that maps each key to full s3 uri
    * @param rdd       An RDD of K, Byte-Array pairs (where the byte-arrays contains image data) to send to S3
    * @param putObjectModifier  Function that will be applied ot S3 PutObjectRequests, so that they can be modified (e.g. to change the ACL settings)
    * @param s3Maker   A function which returns an S3 Client (real or mock) into-which to save the data
    * @param threads   Number of threads dedicated for the IO
    */
  def apply[K](
    dataset: Dataset[(K, Array[Byte])],
    keyToUri: K => String,
    putObjectModifier: PutObjectRequest => PutObjectRequest = { p => p },
    threads: Int = defaultThreadCount
  ): Unit = {
    val keyToPrefix: K => (String, String) = key => {
      val uri = new URI(keyToUri(key))
      require(uri.getScheme == "s3", s"SaveToS3 only supports s3 scheme: $uri")
      val bucket = uri.getAuthority
      val prefix = uri.getPath.substring(1) // drop the leading / from the prefix
      (bucket, prefix)
    }

    dataset.foreachPartition { partition =>
      val s3Client = AmazonS3ClientBuilder.defaultClient()

      val requests: fs2.Stream[IO, PutObjectRequest] =
        fs2.Stream.fromIterator[IO, PutObjectRequest](
          partition.map { case (key, bytes) =>
            val metadata = new ObjectMetadata()
            metadata.setContentLength(bytes.length)
            val is = new ByteArrayInputStream(bytes)
            val (bucket, path) = keyToPrefix(key)
            putObjectModifier(new PutObjectRequest(bucket, path, is, metadata))
          }
        )

      val pool = Executors.newFixedThreadPool(threads)
      implicit val ec = ExecutionContext.fromExecutor(pool)
      implicit val timer: Timer[IO] = IO.timer(ec)
      implicit val cs = IO.contextShift(ec)

      import geotrellis.spark.util.TaskUtils._
      val write: PutObjectRequest => fs2.Stream[IO, PutObjectResult] = { request =>
        fs2.Stream eval IO.shift(ec) *> IO {
          request.getInputStream.reset() // reset in case of retransmission to avoid 400 error
          s3Client.putObject(request)
        }.retryEBO {
          case e: AmazonS3Exception if e.getStatusCode == 503 => true
          case _ => false
        }
      }

      requests
        .map(write)
        .parJoin(threads)
        .compile
        .toVector
        .unsafeRunSync()
      pool.shutdown()
    }
  }
}
