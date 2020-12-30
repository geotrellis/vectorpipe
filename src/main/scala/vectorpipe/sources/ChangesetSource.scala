package vectorpipe.sources

import java.io.{ByteArrayInputStream, IOException}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.zip.GZIPInputStream

import cats.implicits._
import io.circe.generic.auto._
import io.circe.{yaml, _}
import org.apache.commons.io.IOUtils
import org.apache.spark.internal.Logging
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import vectorpipe.model.Changeset
import scalaj.http.Http

import scala.concurrent.duration.{Duration, _}
import scala.util.Try
import scala.xml.XML

object ChangesetSource extends Logging {
  val Delay: Duration = 15 seconds
  // state.yaml uses a custom date format
  private val formatter = DateTimeFormat.forPattern("y-M-d H:m:s.SSSSSSSSS Z")

  private implicit val dateTimeDecoder: Decoder[DateTime] =
    Decoder.instance(a => a.as[String].map(DateTime.parse(_, formatter)))

  def getChangeset(baseURI: URI, sequence: Int, retry: Boolean = true): Seq[Changeset] = {
    val s = f"$sequence%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.osm.gz"

    logDebug(s"Fetching sequence $sequence")

    try {
      val response =
        Http(baseURI.resolve(path).toString).asBytes

      if (response.code == 404) {
        if (retry) {
          logDebug(s"$sequence is not yet available, sleeping.")
          Thread.sleep(Delay.toMillis)
          getChangeset(baseURI, sequence)
        } else {
          logDebug(s"$sequence is yet available")
          Seq()
        }
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

  private def grabSequence(baseURI: URI, filename: String): Sequence = {
    val response =
      Http(baseURI.resolve("state.yaml").toString).asString

    val state = yaml.parser
      .parse(response.body)
      .leftMap(err => err: Error)
      .flatMap(_.as[Sequence])
      .valueOr(throw _)

    state
  }

  def getCurrentSequence(baseURI: URI): Option[Sequence] = {
    var state: Try[Sequence] = null

    for (i <- Range(0, 5)) {
      state = Try(grabSequence(baseURI, "state.yaml"))

      if (state.isSuccess) {
        logDebug(s"$baseURI state: ${state.get.sequence} @ ${state.get.last_run}")

        return Some(state.get)
      }

      Thread.sleep(5000)
    }

    logError("Error fetching / parsing changeset state.", state.failed.get)
    None
  }

  def getSequence(baseURI: URI, sequence: Long): Option[Sequence] = {
    val s = f"${sequence+1}%09d"
    val path = s"${s.slice(0, 3)}/${s.slice(3, 6)}/${s.slice(6, 9)}.state.txt"

    try {
      val state = grabSequence(baseURI, path)

      Some(state)
    } catch {
      case err: Throwable =>
        logError("Error fetching / parsing changeset state.", err)

        None
    }
  }

  def estimateSequenceNumber(modifiedTime: Instant, baseURI: URI, maxIters: Int = 1000): Long = {
    val current = getCurrentSequence(baseURI)
    if (current.isDefined) {
      val diffMinutes = (current.get.last_run.getMillis/1000 -
                         modifiedTime.getEpochSecond) / 60
      current.get.sequence - diffMinutes
    } else {
      // Some queries on the state.yaml fail, set up a failsafe
      // ###.state.txt may not be provided for all replications, so use changsets
      var i = 0
      var baseTime: Long = -1
      while (baseTime == -1 && i < maxIters) {
        baseTime = getChangeset(baseURI, i, false).map(_.createdAt.toInstant.getEpochSecond).sorted.lastOption.getOrElse(-1L)
        i += 1
      }
      if (i == maxIters)
        throw new IndexOutOfBoundsException(s"Couldn't find non-empty changeset in ${maxIters} attempts")

      val query = modifiedTime.getEpochSecond

      (query - baseTime) / 60 + i
    }
  }

  private def safeSequenceTime(baseURI: URI, sequence: Long): Option[Instant] = {
    val res = getSequence(baseURI, sequence)
    if (res.isDefined) {
      Some(Instant.parse(res.get.last_run.toString))
    } else {
      getChangeset(baseURI, sequence.toInt, false).map(_.createdAt.toInstant).sortBy(_.getEpochSecond).lastOption.map{ inst => Instant.parse(inst.toString).plusSeconds(60) }
    }
  }

  def findSequenceFor(modifiedTime: Instant, baseURI: URI): Long = {
    var guess = estimateSequenceNumber(modifiedTime, baseURI)

    while (safeSequenceTime(baseURI, guess).map(_.isAfter(modifiedTime)).getOrElse(false)) { guess -= 1 }
    while (safeSequenceTime(baseURI, guess).map(_.isBefore(modifiedTime)).getOrElse(false)) { guess += 1 }

    guess
  }
}
