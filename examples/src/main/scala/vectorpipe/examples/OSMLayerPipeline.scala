package vectorpipe.examples

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.when
import org.locationtech.jts.{geom => jts}

import vectorpipe._
import vectorpipe.functions.osm._
import vectorpipe.vectortile._

case class OSMLayerPipeline(geometryColumn: String, baseOutputURI: java.net.URI) extends Pipeline {
  val layerMultiplicity = LayerNamesInColumn("layers")

  override val select: Option[(DataFrame, Int, String) => DataFrame] = Some { (wayGeoms: DataFrame, targetZoom: Int, keyColumn: String) =>
    import wayGeoms.sparkSession.implicits._

    wayGeoms
      .withColumn("layers", when(isBuilding('tags), "buildings").when(isRoad('tags), "roads"))
      .where(functions.not(functions.isnull('layers)))
  }

  override val clip: Option[(jts.Geometry, geotrellis.spark.SpatialKey, geotrellis.spark.tiling.LayoutLevel) => jts.Geometry] =
    Some(Clipping.byLayoutCell)
}
