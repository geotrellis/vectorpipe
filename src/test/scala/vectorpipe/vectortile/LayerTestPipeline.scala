package vectorpipe.vectortile

import geotrellis.vector._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.when

import vectorpipe._
import vectorpipe.functions.osm._

case class LayerTestPipeline(geometryColumn: String, baseOutputURI: java.net.URI) extends Pipeline {
  val layerMultiplicity = LayerNamesInColumn("layers")

  override def select(wayGeoms: DataFrame, targetZoom: Int, keyColumn: String): DataFrame = {
    import wayGeoms.sparkSession.implicits._

    wayGeoms
      .withColumn("layers", when(isBuilding('tags), "buildings").when(isRoad('tags), "roads"))
      .where(functions.not(functions.isnull('layers)))
  }

  override def clip(geom: Geometry, key: geotrellis.layer.SpatialKey, layoutLevel: geotrellis.layer.LayoutLevel): Geometry =
    Clipping.byLayoutCell(geom, key, layoutLevel)
}
