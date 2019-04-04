package vectorpipe.vectortile

import geotrellis.raster.RasterExtent
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, explode, lit, not, isnull, sum}
import org.apache.spark.sql.jts.PointUDT
import org.apache.spark.sql.types._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.{geom => jts}

import vectorpipe.TestEnvironment
import vectorpipe._
import vectorpipe.{internal => vp}
import vectorpipe.vectortile._

import org.scalatest._

class PipelineSpec extends FunSpec with TestEnvironment with Matchers {
  import ss.implicits._

  ss.withJTS
  val orcFile = getClass.getResource("/isle-of-man-latest.osm.orc").getPath
  val df = ss.read.orc(orcFile)

  case class TestPipeline(geometryColumn: String, baseOutputURI: java.net.URI, gridResolution: Int) extends Pipeline {
    val weightedCentroid = new WeightedCentroid

    override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
      import input.sparkSession.implicits._

      val layout = layoutLevel.layout
      val binOfTile = functions.udf { (g: jts.Geometry, k: SpatialKey) =>
        val pt = g.asInstanceOf[jts.Point]
        val re = RasterExtent(layout.mapTransform.keyToExtent(k), gridResolution, gridResolution)
        val c = pt.getCoordinate
        re.mapToGrid(c.x, c.y)
      }

      input.withColumn(keyColumn, explode(col(keyColumn)))
        .withColumn("bin", binOfTile(col(geometryColumn), col(keyColumn)))
        .groupBy(col(keyColumn), col("bin"))
        .agg(sum('weight) as 'weight,
             weightedCentroid(col(geometryColumn)) as geometryColumn)
               .drop('bin)
    }

    def pack(row: Row, zoom: Int): VectorTileFeature[Point] = {
      val g = new Point(row.getAs[jts.Point](geometryColumn))
      val weight = row.getAs[Int]("weight")

      Feature(g, Map( "weight" -> VInt64(weight) ))
    }
  }

  describe("Pipeline test") {
    val nodes = vp.preprocessNodes(df, None)
      .filter(functions.not(isnull('lat)))
      .withColumn("geometry", st_makePoint('lon, 'lat))
      .drop("lat", "lon")
      .withColumn("weight", lit(1))
    val pipeline = TestPipeline("geometry", new java.net.URI("file:///tmp/iom-tiles"), 16)

    it("should create a single zoom level") {
      VectorPipe(nodes, pipeline, "points", VectorPipe.Options.forZoom(8))
    }
  }

}
