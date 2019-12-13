package vectorpipe.model

import vectorpipe.model

import org.joda.time.format.ISODateTimeFormat

import io.circe._
import cats.syntax.either._

import java.sql.Timestamp


// TODO is this an AugmentedDiff or an OSM Element w/ a sequence property?
// an AugmentedDiff may be (Option[Element with Sequence], Element with Sequence)
case class ElementWithSequence(id: Long,
                               `type`: String,
                               tags: Map[String, String],
                               nds: Seq[Long],
                               changeset: Long,
                               timestamp: Timestamp,
                               uid: Long,
                               user: String,
                               version: Int,
                               visible: Option[Boolean],
                               sequence: Option[Long]) {
  // TODO extract this; it's used in MakeTiles and elsewhere
  val elementId: String = `type` match {
    case "node"     => s"n$id"
    case "way"      => s"w$id"
    case "relation" => s"r$id"
    case _          => id.toString
  }
}

object ElementWithSequence {
  implicit val decodeFoo: Decoder[ElementWithSequence] = new Decoder[ElementWithSequence] {
    final def apply(c: HCursor): Decoder.Result[ElementWithSequence] =
      for {
        id <- c.downField("id").as[Long]
        `type` <- c.downField("type").as[String]
        tags <- c.downField("tags").as[Map[String, String]]
        nds <- c.downField("nds").as[Option[Seq[Long]]]
        changeset <- c.downField("changeset").as[Long]
        timestampS <- c.downField("timestamp").as[String]
        uid <- c.downField("uid").as[Long]
        user <- c.downField("user").as[String]
        version <- c.downField("version").as[Int]
        visible <- c.downField("visible").as[Option[Boolean]]
        sequence <- c.downField("augmentedDiff").as[Option[Long]]
      } yield {
        val timestamp =
          Timestamp.from(
            ISODateTimeFormat
              .dateTimeParser()
              .parseDateTime(timestampS)
              .toDate
              .toInstant
          )
        model.ElementWithSequence(
          id,
          `type`,
          tags,
          nds.getOrElse(Seq.empty[Long]),
          changeset,
          timestamp,
          uid,
          user,
          version,
          visible,
          sequence
        )
      }
  }
}
