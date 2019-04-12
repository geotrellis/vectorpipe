package vectorpipe.vectortile

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.when
import org.locationtech.jts.{geom => jts}

import vectorpipe._
import vectorpipe.functions.osm._
import vectorpipe.vectortile._

case class LayerTestPipeline(geometryColumn: String, baseOutputURI: java.net.URI) extends Pipeline {
  val layerMultiplicity = LayerNamesInColumn("layers")

  override def select(wayGeoms: DataFrame, targetZoom: Int, keyColumn: String): DataFrame = {
    import wayGeoms.sparkSession.implicits._

    wayGeoms
      .withColumn("layers", when(isBuilding('tags), "buildings").when(isRoad('tags), "roads"))
      .where(functions.not(functions.isnull('layers)))
  }

  override def clip(geom: jts.Geometry, key: geotrellis.spark.SpatialKey, layoutLevel: geotrellis.spark.tiling.LayoutLevel): jts.Geometry =
    Clipping.byLayoutCell(geom, key, layoutLevel)
}
