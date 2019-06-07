package vectorpipe.vectortile

import geotrellis.raster.RasterExtent
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling._
import geotrellis.vector._
import geotrellis.vectortile._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array, col, explode, lit, sum}
import org.apache.spark.sql.types._
import org.locationtech.jts.{geom => jts}

import vectorpipe._
import vectorpipe.vectortile._

case class Bin(x: Int, y: Int)
object Bin {
  def apply(tup: (Int, Int)): Bin = Bin(tup._1, tup._2)
}

case class TestPipeline(geometryColumn: String, baseOutputURI: java.net.URI, gridResolution: Int) extends Pipeline {
  //val weightedCentroid = new WeightedCentroid

  @transient lazy val locallogger = org.apache.log4j.Logger.getLogger(VectorPipe.getClass)

  def time[T](msg: String)(f: => T): T = {
    val s = System.currentTimeMillis
    val result = f
    val e = System.currentTimeMillis
    val t = "%,d".format(e - s)
    locallogger.warn(s"${msg}: Completed in $t ms")
    result
  }

  val layerMultiplicity = SingleLayer("points")
  val gf = new jts.GeometryFactory

  override val reduce: Option[(DataFrame, LayoutLevel, String) => DataFrame] = Some { (input, layoutLevel, keyColumn) =>
    import input.sparkSession.implicits._

    val layout = layoutLevel.layout
    val binOfTile = functions.udf { (g: jts.Geometry, key: GenericRowWithSchema) =>
      val pt = g.asInstanceOf[jts.Point]
      val k = getSpatialKey(key)
      val re = RasterExtent(layout.mapTransform.keyToExtent(k), gridResolution, gridResolution)
      val c = pt.getCoordinate
      Bin(re.mapToGrid(c.x, c.y))
    }
    val st_makePoint = functions.udf { (x: Double, y: Double) => gf.createPoint(new jts.Coordinate(x, y)) }

    val binned = input.withColumn(keyColumn, explode(col(keyColumn)))
      .withColumn("bin", binOfTile(col(geometryColumn), col(keyColumn)))
    time("Binned points into key cells")(binned.count)

    val summed = binned
      .groupBy(col(keyColumn), col("bin"))
      //.agg(sum('weight) as 'weight, weightedCentroid(st_geomToPoint(col(geometryColumn)), 'weight) as geometryColumn)
      .agg(sum('weight) as 'weight, sum('xw) as 'xw, sum('yw) as 'yw)
      .drop('bin)
    time("Summed weighted coords")(summed.count)

    val reduced = summed
      .withColumn(geometryColumn, st_makePoint('xw / 'weight, 'yw / 'weight))
      .withColumn(keyColumn, array(col(keyColumn)))
    time("Converted to points and set keys")(reduced.count)
    reduced
  }

  override def pack(row: Row, zoom: Int): VectorTileFeature[Point] = {
    val g = new Point(row.getAs[jts.Point](geometryColumn))
    val weight = row.getAs[Long]("weight")

    Feature(g, Map( "weight" -> VInt64(weight) ))
  }
}
