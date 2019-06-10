package vectorpipe.examples

import cats.implicits._
import com.monovore.decline._
import geotrellis.proj4.{LatLng, WebMercator, Proj4Transform, Transform}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vector.{Geometry => GTGeometry, Feature}
import geotrellis.vectortile.Value
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{when, udf, not, isnull}
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom._

import vectorpipe._
import vectorpipe.functions.osm._
import vectorpipe.vectortile.{buildVectorTile, Clipping}
import vectorpipe.vectortile.export._

import java.net.URI
import java.nio.file.Path

/*
 spark-submit --class vectorpipe.examples.BaselineMain vectorpipe-examples.jar \
 --output s3://geotrellis-test/jpolchlopek/vectorpipe/vectortiles/layers \
 s3://geotrellis-test/jpolchlopek/vectorpipe/isle-of-man-latest.osm.orc
*/

case class Geom(geom: Geometry, layer: String)
object Geom {
  val st_reproject = udf { g: Geometry =>
    val trans = Proj4Transform(LatLng, WebMercator)
    GTGeometry(g).reproject(trans).jtsGeom
  }
}

case class KeyedGeom(key: SpatialKey, layer: String, geom: Geometry)
object KeyedGeom {
  def fromGeom(layout: LayoutDefinition)(g: Geom): Seq[KeyedGeom] = {
    layout.mapTransform.keysForGeometry(GTGeometry(g.geom.asInstanceOf[Geometry])).map{ k => KeyedGeom(k, g.layer, g.geom) }.toSeq
  }
}

object BaselineMain extends CommandApp(
  name = "OSM-layers",
  header = "Convert OSM ORC file into vector tile set containing roads and buildings",
  main = {
    val outputOpt = Opts.option[URI]("output", help = "Base URI for output.")
    val inputOpt = Opts.argument[String]()

    (outputOpt, inputOpt).mapN { (output, input) =>
      implicit val spark: SparkSession = SparkSession.builder
        .appName("Layer VT Generation")
        .config("spark.ui.enabled", "true")
        .config("spark.default.parallelism","8")
        .config("spark.serializer", classOf[KryoSerializer].getName)
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.kryoserializer.buffer.max", "500m")
        .config("spark.sql.orc.impl", "native")
        .getOrCreate()
        .withJTS

      import spark.implicits._

      val level = ZoomedLayoutScheme(WebMercator).levelForZoom(14)
      val layout = level.layout

      val df = spark.read.orc(input)
      val keyedGeoms = OSM
        .toGeometry(df)
        .withColumn("layer", when(isBuilding('tags), "buildings").when(isRoad('tags), "roads"))
        .where(not(isnull('layer)))
        // .count
        .withColumn("geom", Geom.st_reproject('geom))
        .as[Geom]
        .flatMap(KeyedGeom.fromGeom(layout)(_))
      val grouped = keyedGeoms.groupByKey(_.key)
      val tiles = grouped.mapGroups{ (key, geoms) =>
        val clipped = geoms.map{ klg => KeyedGeom(key, klg.layer, Clipping.byLayoutCell(klg.geom, key, level)) }
        val layers = clipped.toIterable.groupBy(_.layer)
        val features = layers.mapValues{ _.map{ klg => Feature(GTGeometry(klg.geom), Map.empty[String, Value]) } }
        key -> buildVectorTile(features, layout.mapTransform.keyToExtent(key), 4096, false).toBytes
      }

      saveVectorTiles(tiles, 14, output)
      // System.err.println(keyedGeoms)

      spark.stop
    }
  }

)
