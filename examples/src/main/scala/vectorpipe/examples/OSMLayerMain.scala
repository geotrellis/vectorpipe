package vectorpipe.examples

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts._

import vectorpipe._

import java.net.URI
import java.nio.file.Path

/*
 spark-submit --class vectorpipe.examples.OSMLayerMain vectorpipe-examples.jar \
 --output s3://geotrellis-test/jpolchlopek/vectorpipe/vectortiles/layers \
 s3://geotrellis-test/jpolchlopek/vectorpipe/isle-of-man-latest.osm.orc
*/

object OSMLayerMain extends CommandApp(
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

      val df = spark.read.orc(input)
      val wayGeoms = OSM.toGeometry(df)
      val pipeline = OSMLayerPipeline("geom", output)

      VectorPipe(wayGeoms, pipeline, VectorPipe.Options.forZoom(14))

      spark.stop
    }
  }

)
