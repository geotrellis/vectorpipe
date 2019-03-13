package vectorpipe.util

import com.vividsolutions.jts.{geom => jts}
import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spray.json._

object Geocode {

  case class CountryId(code: String)

  object MyJsonProtocol extends DefaultJsonProtocol {
    implicit object CountryIdJsonFormat extends RootJsonFormat[CountryId] {
      def read(value: JsValue): CountryId =
        value.asJsObject.getFields("ADM0_A3") match {
          case Seq(JsString(code)) =>
            CountryId(code)
          case v =>
            throw DeserializationException(s"CountryId expected, got $v")
        }

      def write(v: CountryId): JsValue =
        JsObject(
          "code" -> JsString(v.code)
        )
    }
  }

  import MyJsonProtocol._

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
      SpatialIndex.fromExtents(all) { mpf => mpf.geom.envelope }
  }

  class CountryLookup() extends Serializable {
    private val index =
      geotrellis.vector.SpatialIndex.fromExtents(
        Countries.all.
          map { mpf =>
            (mpf.geom.prepare, mpf.data)
          }
      ) { case (pg, _) => pg.geom.envelope }

    def lookup(geom: geotrellis.vector.Geometry): Traversable[CountryId] = {
      val t =
        new Traversable[(geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)] {
          override def foreach[U](f: ((geotrellis.vector.prepared.PreparedGeometry[geotrellis.vector.MultiPolygon],
            CountryId)) => U): Unit = {
            val visitor = new com.vividsolutions.jts.index.ItemVisitor {
              override def visitItem(obj: AnyRef): Unit = f(obj.asInstanceOf[(geotrellis.vector.prepared
              .PreparedGeometry[geotrellis.vector.MultiPolygon], CountryId)])
            }
            index.rtree.query(geom.jtsGeom.getEnvelopeInternal, visitor)
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
          val countryCodes = Option(row.getAs[jts.Geometry]("geom")).map(Geometry(_)) match {
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
