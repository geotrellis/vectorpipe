package vectorpipe.util

import geotrellis.vector._
import geotrellis.vector.io.json._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.locationtech.jts.geom.prep._
import org.locationtech.jts.index.ItemVisitor

import _root_.io.circe.{Encoder => CirceEncoder, Decoder => CirceDecoder, _}
import cats.syntax.either._

object Geocode {

  case class CountryId(code: String)

  object CountryIdCodecs {
    implicit val encodeCountryId: CirceEncoder[CountryId] = new CirceEncoder[CountryId] {
      final def apply(a: CountryId): Json = Json.obj(
        ("code", Json.fromString(a.code))
      )
    }
    implicit val decodeCountryId: CirceDecoder[CountryId] = new CirceDecoder[CountryId] {
      final def apply(c: HCursor): CirceDecoder.Result[CountryId] =
        for {
          code <- c.downField("ADM0_A3").as[String]
        } yield {
          CountryId(code)
        }
    }
  }

  import CountryIdCodecs._

  object Countries {
    lazy val all: Vector[MultiPolygonFeature[CountryId]] = {
      val collection =
        Resource("countries.geojson").
          parseGeoJson[JsonFeatureCollection]

      val polys =
        collection.
          getAllPolygonFeatures[CountryId].
          map(_.mapGeom(MultiPolygon(_)))

      val mps =
        collection.
          getAllMultiPolygonFeatures[CountryId]

      polys ++ mps
    }

    def indexed: SpatialIndex[MultiPolygonFeature[CountryId]] =
      SpatialIndex.fromExtents(all) { mpf => mpf.geom.extent }
  }

  class CountryLookup() extends Serializable {
    private val index =
      geotrellis.vector.SpatialIndex.fromExtents(
        Countries.all.
          map { mpf =>
            (PreparedGeometryFactory.prepare(mpf.geom), mpf.data)
          }
      ) { case (pg, _) => pg.getGeometry().extent }

    def lookup(geom: geotrellis.vector.Geometry): Traversable[CountryId] = {
      val t =
        new Traversable[(PreparedGeometry, CountryId)] {
          override def foreach[U](f: ((PreparedGeometry, CountryId)) => U): Unit = {
            val visitor = new ItemVisitor {
              override def visitItem(obj: AnyRef): Unit =
                f(obj.asInstanceOf[(PreparedGeometry, CountryId)])
            }
            index.rtree.query(geom.getEnvelopeInternal, visitor)
          }
        }

      t.
        filter(_._1.intersects(geom)).
        map(_._2)
    }
  }

  def apply(geoms: DataFrame): DataFrame = {
    val newSchema = StructType(geoms.schema.fields :+ StructField(
      "countries", ArrayType(StringType, containsNull = false), nullable = true))
    implicit val encoder: Encoder[Row] = RowEncoder(newSchema)

    geoms
      .mapPartitions { partition =>
        val countryLookup = new CountryLookup()

        partition.map { row =>
          val countryCodes = Option(row.getAs[Geometry]("geom")) match {
            case Some(geom) => countryLookup.lookup(geom).map(x => x.code)
            case None => Seq.empty[String]
          }

          Row.fromSeq(row.toSeq :+ countryCodes)
        }
      }
  }

  def regionsByChangeset(geomCountries: Dataset[Row]): DataFrame = {
    import geomCountries.sparkSession.implicits._

    geomCountries
      .where('country.isNotNull)
      .groupBy('changeset)
      .agg(collect_set('country) as 'countries)

  }

}
