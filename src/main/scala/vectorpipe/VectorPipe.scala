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
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.locationtech.jts.{geom => jts}

object VectorPipe {

  case class Options(
    maxZoom: Int,                    // Largest (most resolute) zoom level to generate
    minZoom: Option[Int],            // Smallest (least resolute) zoom level to generate
    srcCRS: CRS,                     // projection of the original geometry
    destCRS: Option[CRS],            // projection for generated vectortiles
    tileResolution: Int = 4096       // "pixel" resolution of output vectortiles
  )
  object Options {
    def forZoom(zoom: Int) = Options(zoom, None, LatLng, None)
    def forZoomRange(maxZoom: Int, minZoom: Int) = Options(maxZoom, Some(minZoom), LatLng, None)
    def forAllZoomsFrom(zoom: Int) = Options(zoom, Some(0), LatLng, None)
    def forAllZoomsWithSrcProjection(zoom: Int, crs: CRS) = Options(zoom, Some(0), crs, None)
  }

  def apply(input: DataFrame, pipeline: vectortile.Pipeline, layerName: String, options: Options): Unit = {
    val geomColumn = pipeline.geometryColumn
    assert(input.columns.contains(geomColumn) &&
           input.schema(geomColumn).dataType.isInstanceOf[org.apache.spark.sql.jts.AbstractGeometryUDT[jts.Geometry]],
           s"Input DataFrame must contain a column `${geomColumn}` of JTS Geometry")

    val srcCRS = options.srcCRS
    val destCRS = options.destCRS.getOrElse(WebMercator)
    val maxZoom = options.maxZoom
    val minZoom = math.min(math.max(0, options.minZoom.getOrElse(options.maxZoom)), options.maxZoom)
    val zls = ZoomedLayoutScheme(destCRS, options.tileResolution)

    // Reproject geometries if needed
    val reprojected = input.withColumn(geomColumn, st_reprojectGeom(col(geomColumn), lit(srcCRS.toProj4String), lit(destCRS.toProj4String)))

    // Prefilter data for first iteration and key geometries to initial layout
    val keyColumn = {
      var prepend = "_"
      while (input.columns.contains(prepend ++ "keys")) { prepend = "_" ++ prepend }
      prepend ++ "keys"
    }

    def reduceKeys = udf { seq: Seq[Row] =>
      seq.toSet.map{ r: Row => SpatialKey(r.getAs[Int]("col") / 2, r.getAs[Int]("row") / 2) }.toSeq
    }

    def generateVectorTiles[G <: Geometry](df: DataFrame, level: LayoutLevel): RDD[(SpatialKey, VectorTile)] = {
      val zoom = level.zoom
      val clip = udf { (g: jts.Geometry, key: GenericRowWithSchema) =>
        val k = SpatialKey(key.getInt(0), key.getInt(1))
        pipeline.clip(g, k, level)
      }

      pipeline
        .select(df, zoom)
        .withColumn(keyColumn, explode(col(keyColumn)))
        .repartition(col(keyColumn)) // spread copies of possibly ill-tempered geometries around cluster
        .withColumn(geomColumn, clip(col(geomColumn), col(keyColumn)))
        .rdd
        .map { r => (r.getAs[GenericRowWithSchema](keyColumn), pipeline.pack(r, zoom)) }
        .groupByKey
        .map { case (r, feats) =>
          val k = SpatialKey(r.getInt(0), r.getInt(1))
          val ex = level.layout.mapTransform.keyToExtent(k)
          (k, buildVectorTile(feats, ex, layerName, options.tileResolution))
        }
    }

    // ITERATION:
    // 1.   Select
    // 2.   Clip
    // 3.   Pack
    // 4.   Generate vector tiles
    // 5.   Reduce
    // 6.   Simplify
    // 7.   Re-key

    Range.Int(maxZoom, minZoom, -1).inclusive.foldLeft(reprojected){ (df, zoom) =>
      val level = zls.levelForZoom(zoom)
      val working =
        if (zoom == maxZoom) {
          df.withColumn(keyColumn, keyTo(level.layout)(col(geomColumn)))
        } else {
          df
        }
      val simplify = udf { g: jts.Geometry => pipeline.simplify(g, level.layout) }
      val reduced = pipeline
        .reduce(working, level, keyColumn)
      val prepared = reduced
        .withColumn(geomColumn, simplify(col(geomColumn)))
      val vts = generateVectorTiles(prepared, level)
      saveVectorTiles(vts, zoom, pipeline.baseOutputURI)
      prepared.withColumn(keyColumn, reduceKeys(col(keyColumn)))
    }

  }

}
