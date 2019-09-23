package vectorpipe.sources

import java.io.{ByteArrayInputStream, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.zip.GZIPInputStream

import cats.implicits._
import com.softwaremill.macmemo.memoize
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import vectorpipe.model.Changeset
import scalaj.http.Http

import scala.concurrent.duration.{Duration, _}
import scala.xml.XML

object ChangesetSource extends Logging {
  val Delay: Duration = 15 seconds
  // state.yaml uses a custom date format
  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))

  def getChangeset(baseURI: URI, sequence: Int): Seq[Changeset] = {
    val s = f"$sequence%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.osm.gz"

    logDebug(s"Fetching sequence $sequence")

    try {
      val response =
        Http(baseURI.resolve(path).toString).asBytes

      if (response.code == 404) {
        logDebug(s"$sequence is not yet available, sleeping.")
        Thread.sleep(Delay.toMillis)
        getChangeset(baseURI, sequence)
      } else {
        // NOTE: if diff bodies get really large, switch to a SAX parser to help with the memory footprint
        val bais = new ByteArrayInputStream(response.body)
        val gzis = new GZIPInputStream(bais)
        try {
          val data = XML.loadString(IOUtils.toString(gzis, StandardCharsets.UTF_8))

          val changesets = (data \ "changeset").map(Changeset.fromXML(_, sequence))

          logDebug(s"Received ${changesets.length} changesets")

          changesets
        } finally {
          gzis.close()
          bais.close()
        }
      }
    } catch {
      case e: IOException =>
        logWarning(s"Error fetching changeset $sequence", e)
        Thread.sleep(Delay.toMillis)
        getChangeset(baseURI, sequence)
    }
  }

  case class Sequence(last_run: DateTime, sequence: Long)

  @memoize(maxSize = 1, expiresAfter = 30 seconds)
  def getCurrentSequence(baseURI: URI): Option[Sequence] = {
    try {
      val response =
        Http(baseURI.resolve("state.yaml").toString).asString

      val state = yaml.parser
        .parse(response.body)
        .leftMap(err => err: Error)
        .flatMap(_.as[Sequence])
        .valueOr(throw _)

      logDebug(s"$baseURI state: ${state.sequence} @ ${state.last_run}")

      Some(state)
    } catch {
      case err: Throwable =>
        logError("Error fetching / parsing changeset state.", err)

        None
    }
  }

  def getSequence(baseURI: URI, sequence: Long): Option[Sequence] = {
    val s = f"${sequence+1}%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.state.txt"

    try {
      val response =
        Http(baseURI.resolve(path).toString).asString

      val state = yaml.parser
        .parse(response.body)
        .leftMap(err => err: Error)
        .flatMap(_.as[Sequence])
        .valueOr(throw _)

      Some(state)
    } catch {
      case err: Throwable =>
        logError("Error fetching / parsing changeset state.", err)

        None
    }
  }

  def estimateSequenceNumber(modifiedTime: Instant, baseURI: URI): Long = {
    val current = getCurrentSequence(baseURI)
    val diffMinutes = (current.get.last_run.toInstant.getMillis/1000 - modifiedTime.getEpochSecond) / 60
    current.get.sequence - diffMinutes
  }

  def findSequenceFor(modifiedTime: Instant, baseURI: URI): Long = {
    var guess = estimateSequenceNumber(modifiedTime, baseURI)
    val target = org.joda.time.Instant.parse(modifiedTime.toString)

    while (getSequence(baseURI, guess).get.last_run.isAfter(target)) { guess -= 1 }
    while (getSequence(baseURI, guess).get.last_run.isBefore(target)) { guess += 1 }

    getSequence(baseURI, guess).get.sequence
  }
}
