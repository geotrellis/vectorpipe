package vectorpipe

import vectorpipe.vectortile._
import vectorpipe.vectortile.export._

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.{ZoomedLayoutScheme, LayoutLevel}
import geotrellis.vector.Geometry
import geotrellis.vectortile._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.jts.{geom => jts}

object VectorPipe {

  case class Options(
    maxZoom: Int,                    // Largest (most resolute) zoom level to generate
    minZoom: Option[Int],            // Smallest (least resolute) zoom level to generate
    srcCRS: CRS,                     // projection of the original geometry
    destCRS: Option[CRS],            // projection for generated vectortiles
    tileResolution: Int = 4096,      // "pixel" resolution of output vectortiles
    geometryColumn: String = "geom"  // name of the dataframe column containing geometries
  )
  object Options {
    def forZoom(zoom: Int) = Options(zoom, None, LatLng, None)
    def forAllZoomsFrom(zoom: Int) = Options(zoom, Some(0), LatLng, None)
    def forAllZoomsWithSrcProjection(zoom: Int, crs: CRS) = Options(zoom, Some(0), crs, None)
  }

  def apply(input: DataFrame, pipeline: vectortile.Pipeline, layerName: String, options: Options): Unit = {
    val geomColumn = options.geometryColumn
    assert(input.columns.contains(geomColumn), s"Input DataFrame must contain a column `${geomColumn}` of JTS Geometry")
    // TODO check the type of the geometry column

    val srcCRS = options.srcCRS
    val destCRS = options.destCRS.getOrElse(WebMercator)
    val maxZoom = options.maxZoom
    val minZoom = math.min(math.max(0, options.minZoom.getOrElse(options.maxZoom)), options.maxZoom)
    val zls = ZoomedLayoutScheme(destCRS, options.tileResolution)

    // Reproject geometries if needed
    val reprojected = input.withColumn(options.geometryColumn,
                                       st_reprojectGeom(col(options.geometryColumn),
                                                        lit(srcCRS.toProj4String),
                                                        lit(destCRS.toProj4String)))

    // Prefilter data for first iteration and key geometries to initial layout
    val keyColumn = {
      var prepend = "_"
      while (input.columns.contains(prepend ++ "keys")) { prepend = "_" ++ prepend }
      prepend ++ "keys"
    }

    def generateVectorTiles[G <: Geometry](df: DataFrame, level: LayoutLevel): RDD[(SpatialKey, VectorTile)] = {
      val zoom = level.zoom
      val clip = udf { (g: jts.Geometry, k: SpatialKey) => pipeline.clip(g, k, level) }

      pipeline
        .select(df, zoom)
        .withColumn(keyColumn, explode(col(keyColumn)))
        .repartition(col(keyColumn)) // spread copies of possibly ill-tempered geometries around cluster
        .withColumn(geomColumn, clip(col(geomColumn)))
        .rdd
        .map { r => (r.getAs[SpatialKey](keyColumn), pipeline.pack(r, zoom)) }
        .groupByKey
        .map { case (k, feats) =>
          val ex = level.layout.mapTransform.keyToExtent(k)
          (k, buildVectorTile(feats, ex, layerName, options.tileResolution))
        }
    }

    // ITERATION:
    // 1.   Select
    // 2.   Clip
    // 3.   Pack
    // 4.   Generate vector tiles
    // 5. TODO Re-key
    // 6.   Reduce
    // 7.   Simplify

    Range.Int(maxZoom, minZoom, -1).inclusive.foldLeft(reprojected){ (df, zoom) =>
      val level = zls.levelForZoom(zoom)
      val working =
        if (zoom == maxZoom) {
          val keyToBase = udf { g: jts.Geometry => level.layout.mapTransform.keysForGeometry(geotrellis.vector.Geometry(g)).toArray }
          df.withColumn(keyColumn, keyToBase(col(geomColumn)))
        } else {
          df
        }
      val simplify = udf { g: jts.Geometry => pipeline.simplify(g, level.layout) }
      val prepared = pipeline
        .reduce(working, level)
        .withColumn(geomColumn, simplify(col(geomColumn)))
      val vts = generateVectorTiles(prepared, level)
      saveVectorTiles(vts, zoom, pipeline.baseOutputURI)
      prepared
    }

  }

}
