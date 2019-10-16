package vectorpipe.vectortile

import geotrellis.raster.RasterExtent
import geotrellis.layer._
import geotrellis.vector._
import geotrellis.vectortile._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array, col, explode, sum}

import vectorpipe._

case class Bin(x: Int, y: Int)
object Bin {
  def apply(tup: (Int, Int)): Bin = Bin(tup._1, tup._2)
}

case class TestPipeline(geometryColumn: String, baseOutputURI: java.net.URI, gridResolution: Int) extends Pipeline {
  val weightedCentroid = new WeightedCentroid

  val layerMultiplicity = SingleLayer("points")

  override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
    import input.sparkSession.implicits._

    val layout = layoutLevel.layout
    val binOfTile = functions.udf { (g: Geometry, key: GenericRowWithSchema) =>
      val pt = g.asInstanceOf[Point]
      val k = getSpatialKey(key)
      val re = RasterExtent(layout.mapTransform.keyToExtent(k), gridResolution, gridResolution)
      val c = pt.getCoordinate
      Bin(re.mapToGrid(c.x, c.y))
    }

    val st_geomToPoint = functions.udf { g: Geometry => g.asInstanceOf[Point] }

    input.withColumn(keyColumn, explode(col(keyColumn)))
      .withColumn("bin", binOfTile(col(geometryColumn), col(keyColumn)))
      .groupBy(col(keyColumn), col("bin"))
      .agg(sum('weight) as 'weight, weightedCentroid(st_geomToPoint(col(geometryColumn)), 'weight) as geometryColumn)
      .drop('bin)
      .withColumn(keyColumn, array(col(keyColumn)))
  }

  override def pack(row: Row, zoom: Int): VectorTileFeature[Point] = {
    val g = row.getAs[Point](geometryColumn)
    val weight = row.getAs[Long]("weight")

    Feature(g, Map( "weight" -> VInt64(weight) ))
  }
}
