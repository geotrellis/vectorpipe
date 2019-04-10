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
import org.apache.spark.sql.types.StringType
import org.locationtech.jts.{geom => jts}

object VectorPipe {

  /** Vectortile conversion options.
    *
    * @param  maxZoom             Largest (most resolute) zoom level to generate.
    * @param  minZoom             (optional) Smallest (least resolute) zoom level to generate.  When
    *                             omitted, only generate the single level for maxZoom.
    * @param  srcCRS              CRS of the original geometry
    * @param  destCRS             (optional) The CRS to produce vectortiles into.  When omitted,
    *                             defaults to [[WebMercator]].
    * @param  orderAreas          Sorts polygonal geometries in vectortiles.  In case of overlaps,
    *                             smaller geometries will draw on top of larger ones.
    * @param  tileResolution      Resolution of output tiles; i.e., the number of discretized bins
    *                             (along each axis) to quantize coordinates to for display.
    *
    */
  case class Options(
    maxZoom: Int,
    minZoom: Option[Int],
    srcCRS: CRS,
    destCRS: Option[CRS],
    orderAreas: Boolean = false,
    tileResolution: Int = 4096
  )
  object Options {
    def forZoom(zoom: Int) = Options(zoom, None, LatLng, None)
    def forZoomRange(minZoom: Int, maxZoom: Int) = Options(maxZoom, Some(minZoom), LatLng, None)
    def forAllZoomsFrom(zoom: Int) = Options(zoom, Some(0), LatLng, None)
    def forAllZoomsWithSrcProjection(zoom: Int, crs: CRS) = Options(zoom, Some(0), crs, None)
  }

  def apply(input: DataFrame, pipeline: vectortile.Pipeline, options: Options): Unit = {
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

    def reduceKeys = udf { seq: Seq[GenericRowWithSchema] =>
      seq.toSet.map{ r: GenericRowWithSchema =>
        val k = getSpatialKey(r)
        SpatialKey(k.col / 2, k.row / 2) }.toSeq
    }

    def generateVectorTiles[G <: Geometry](df: DataFrame, level: LayoutLevel): RDD[(SpatialKey, VectorTile)] = {
      val zoom = level.zoom
      val clip = udf { (g: jts.Geometry, key: GenericRowWithSchema) =>
        val k = getSpatialKey(key)
        pipeline.clip(g, k, level)
      }

      val selectedGeometry = pipeline
        .select(df, zoom, keyColumn)

      val clipped = selectedGeometry
        .withColumn(keyColumn, explode(col(keyColumn)))
        .repartition(col(keyColumn)) // spread copies of possibly ill-tempered geometries around cluster prior to clipping
        .withColumn(geomColumn, clip(col(geomColumn), col(keyColumn)))

      if (pipeline.layerNameIsColumn) {
        assert(selectedGeometry.schema(pipeline.layerName).dataType == StringType,
               s"layerNameIsColumn=true implies select must produce a String-type column of name ${pipeline.layerName}")
        clipped
          .rdd
          .map { r => (getSpatialKey(r, keyColumn), r.getAs[String](pipeline.layerName) -> pipeline.pack(r, zoom)) }
          .groupByKey
          .mapPartitions{ iter: Iterator[(SpatialKey, Iterable[(String, VectorTileFeature[Geometry])])] =>
            iter.map{ case (key, groupedFeatures) => {
              val layerFeatures: Map[String, Iterable[VectorTileFeature[Geometry]]] =
                groupedFeatures.groupBy(_._1).mapValues(_.map(_._2))
              val ex = level.layout.mapTransform.keyToExtent(key)
              key -> buildVectorTile(layerFeatures, ex, options.tileResolution, options.orderAreas)
            }}
          }
      } else {
        clipped
          .rdd
          .map { r => (getSpatialKey(r, keyColumn), pipeline.pack(r, zoom)) }
          .groupByKey
          .map { case (key, feats) =>
            val ex = level.layout.mapTransform.keyToExtent(key)
            key -> buildVectorTile(feats, pipeline.layerName, ex, options.tileResolution, options.orderAreas)
          }
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
