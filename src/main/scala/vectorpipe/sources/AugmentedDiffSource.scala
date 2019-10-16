package vectorpipe.sources

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant
import java.util.zip.GZIPInputStream

import geotrellis.store.s3._
import geotrellis.vector.io.json.JsonFeatureCollectionMap
import geotrellis.vector.io.json.Implicits._
import geotrellis.vector.{Feature, Geometry}

import vectorpipe.model.{AugmentedDiff, ElementWithSequence}

import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{floor, from_unixtime, to_timestamp, unix_timestamp}

import io.circe._
import io.circe.generic.auto._
import cats.implicits._

import com.amazonaws.services.s3.model.AmazonS3Exception
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.S3Client
import com.softwaremill.macmemo.memoize
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, _}


object AugmentedDiffSource extends Logging {
  private lazy val s3: S3Client = S3ClientProducer.get()
  val Delay: Duration = 15.seconds

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse))

  def getSequence(baseURI: URI, sequence: Int): Seq[AugmentedDiff] = {
    val bucket = baseURI.getHost
    val prefix = new File(baseURI.getPath.drop(1)).toPath
    // left-pad sequence
    val s = f"$sequence%09d"
    val key = prefix.resolve(s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.json.gz").toString

    logDebug(s"Fetching sequence $sequence")

    try {
      val request = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()

      val response = s3.getObjectAsBytes(request)
      val gzis = new GZIPInputStream(response.asInputStream)

      try {
        IOUtils
          .toString(gzis, StandardCharsets.UTF_8)
          .lines
          .map { line =>
            // Spark doesn't like RS-delimited JSON; perhaps Spray doesn't either
            val features = line
              .replace("\u001e", "")
              .parseGeoJson[JsonFeatureCollectionMap]
              .getAll[Feature[Geometry, ElementWithSequence]]

            AugmentedDiff(sequence, features.get("old"), features("new"))
          }
          .toSeq
      } finally {
        gzis.close()
      }
    } catch {
      case e: AmazonS3Exception if e.getStatusCode == 404 || e.getStatusCode == 403 =>
        getCurrentSequence(baseURI) match {
          case Some(s) if s > sequence =>
            logInfo("Encountered missing sequence, comparing with current for validity")
            // sequence is missing; this is intentional, so compare with currentSequence for validity
            Seq.empty[AugmentedDiff]
          case _ =>
            logInfo(s"$sequence is not yet available, sleeping.")
            Thread.sleep(Delay.toMillis)
            getSequence(baseURI, sequence)
        }
      case t: Throwable =>
        logError(s"sequence $sequence caused an error", t)
        Thread.sleep(Delay.toMillis)
        getSequence(baseURI, sequence)
    }
  }

  @memoize(maxSize = 1, expiresAfter = 30 seconds)
  def getCurrentSequence(baseURI: URI): Option[Int] = {
    val bucket = baseURI.getHost
    val prefix = new File(baseURI.getPath.drop(1)).toPath
    val key = prefix.resolve("state.yaml").toString

    try {
      val request = GetObjectRequest.builder()
        .bucket(bucket)
        .key(key)
        .build()
      val response = s3.getObjectAsBytes(request)

      val body = IOUtils
        .toString(response.asInputStream, StandardCharsets.UTF_8.toString)

      val state = yaml.parser
        .parse(body)
        .leftMap(err => err: Error)
        .flatMap(_.as[State])
        .valueOr(throw _)

      logDebug(s"$baseURI state: ${state.sequence} @ ${state.last_run}")

      Some(state.sequence)
    } catch {
      case err: Throwable =>
        logError("Error fetching / parsing changeset state.", err)

        None
    }
  }

  def timestampToSequence(timestamp: Timestamp): Int =
    ((timestamp.toInstant.getEpochSecond - 1347432900) / 60).toInt

  def timestampToSequence(timestamp: Column): Column =
    floor((unix_timestamp(timestamp) - 1347432900) / 60)

  def sequenceToTimestamp(sequence: Int): Timestamp =
    Timestamp.from(Instant.ofEpochSecond(sequence.toLong * 60 + 1347432900L))

  def sequenceToTimestamp(sequence: Column): Column =
    to_timestamp(from_unixtime(sequence * 60 + 1347432900))

  case class State(last_run: DateTime, sequence: Int)
}
