package vectorpipe.sources

import java.io.{BufferedInputStream, File}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant
import java.util.zip.GZIPInputStream

import geotrellis.store.s3._
import geotrellis.vector._

import vectorpipe.model.{AugmentedDiff, ElementWithSequence}
import vectorpipe.util._
//import vectorpipe.util.RobustFeatureFormats._

import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{floor, from_unixtime, to_timestamp, unix_timestamp}

import _root_.io.circe._
import _root_.io.circe.generic.auto._
import cats.implicits._

import software.amazon.awssdk.services.s3.model.{GetObjectRequest, NoSuchKeyException, S3Exception}
import software.amazon.awssdk.services.s3.S3Client
import com.softwaremill.macmemo.memoize
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, _}


object AugmentedDiffSource extends Logging {
  type RF = RobustFeature[Geometry, ElementWithSequence]

  private lazy val s3: S3Client = S3ClientProducer.get()
  val Delay: Duration = 15.seconds

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse))

  def getFeatures(baseURI: URI, sequence: Int): Seq[Map[String, RF]] = {
    val bucket = baseURI.getHost
    val prefix = new File(baseURI.getPath.drop(1)).toPath
    // left-pad sequence
    val s = f"$sequence%09d"
    val key = prefix.resolve(s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.json.gz").toString

    logDebug(s"Fetching sequence $sequence")

    val obj = s3.getObject(
      GetObjectRequest
        .builder
        .bucket(bucket)
        .key(key)
        .build
    )

    val bis = new BufferedInputStream(obj)
    val gzis = new GZIPInputStream(bis)

    try {
      IOUtils
        .toString(gzis, StandardCharsets.UTF_8)
        .lines
        .map { line =>
          // Spark doesn't like RS-delimited JSON; perhaps Spray doesn't either
          line
            .replace("\u001e", "")
            .parseGeoJson[JsonRobustFeatureCollectionMap]
            .getAll[RF]
        }
        .toSeq
    } finally {
      gzis.close()
      bis.close()
    }
  }

  /**
   * Fetch all augmented diffs from a sequence number.
   *
   * This function collects the data in an augmented diff sequence file into
   * vectorpipe.model.AugmentedDiff objects.  These diff files are expected to be
   * stored on S3 in .json.gz files.  This method provides the option to process errors
   * generated when the new geometry in the diff is faulty.  If `waitUntilAvailable` is
   * set to true, the process will block, in 15 second increments, until the sequence
   * file is available.
   */
  def getSequence(baseURI: URI, sequence: Int, badGeometryHandler: (Int, RF) => Unit, waitUntilAvailable: Boolean): Seq[AugmentedDiff] = {
    logDebug(s"Fetching sequence $sequence")

    try {
      val robustFeatureMaps = getFeatures(baseURI, sequence)

      robustFeatureMaps.map{ m =>
        if (m.contains("new") && !m("new").geom.isDefined) badGeometryHandler(sequence, m("new"))
        AugmentedDiff(sequence, m.get("old").map(_.toFeature), m("new").toFeature)
      }
    } catch {
      case e: S3Exception if e.isInstanceOf[NoSuchKeyException] || e.statusCode == 403 =>
        logInfo(s"Encountered missing sequence (baseURI = ${baseURI}, sequence = ${sequence}), comparing with current for validity")
        getCurrentSequence(baseURI) match {
          case Some(s) if s > sequence =>
            logInfo(s"$sequence is missing, continuing")
            Seq.empty[AugmentedDiff]
          case _ =>
            if (waitUntilAvailable) {
              logInfo(s"$sequence is not yet available, sleeping.")
              Thread.sleep(Delay.toMillis)
              getSequence(baseURI, sequence, waitUntilAvailable)
            } else
              throw e
        }
      case t: Throwable =>
        if (waitUntilAvailable) {
          logError(s"sequence $sequence caused an error", t)
          Thread.sleep(Delay.toMillis)
          getSequence(baseURI, sequence)
        } else
          throw t
    }
  }

  def getSequence(baseURI: URI, sequence: Int): Seq[AugmentedDiff] =
    getSequence(baseURI, sequence, {(_: Int, _: RF) => ()}, true)

  def getSequence(baseURI: URI, sequence: Int, waitUntilAvailable: Boolean): Seq[AugmentedDiff] =
    getSequence(baseURI, sequence, {(_: Int, _: RF) => ()}, waitUntilAvailable)

  def getSequence(baseURI: URI, sequence: Int, badGeometryHandler: (Int, RF) => Unit): Seq[AugmentedDiff] =
    getSequence(baseURI, sequence, badGeometryHandler, true)

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
