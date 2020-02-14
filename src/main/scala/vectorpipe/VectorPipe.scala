package vectorpipe

import vectorpipe.vectortile._
import vectorpipe.vectortile.export._

import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.layer._
import geotrellis.spark.store.kryo.KryoRegistrator
import geotrellis.vector._
import geotrellis.vectortile._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import org.locationtech.geomesa.spark.jts.SparkSessionWithJTS

object VectorPipe {

  /** Vectortile conversion options.
    *
    * @param  layoutLevels          Zoom layout levels to generate.  Each level must define a unique zoom,
    *                               and the tile layout for each level must be square (tileCols == tileRows).
    * @param  srcCRS                CRS of the original geometry
    * @param  destCRS               The CRS to produce vectortiles into
    * @param  useCaching            Allows intermediate results to be cached to disk.  May require
    *                               additional disk space on executor nodes.
    * @param  orderAreas            Sorts polygonal geometries in vectortiles.  In case of overlaps,
    *                               smaller geometries will draw on top of larger ones.
    */
  case class Options(
    layoutLevels: Seq[LayoutLevel],
    srcCRS: CRS,
    destCRS: CRS,
    useCaching: Boolean,
    orderAreas: Boolean
  )
  object Options {
    /** Vectortile conversion options using [[geotrellis.layer.ZoomedLayoutScheme ZoomedLayoutScheme]]
      *
      * @param  maxZoom             Largest (most resolute) zoom level to generate.
      * @param  minZoom             (optional) Smallest (least resolute) zoom level to generate.  When
      *                             omitted, only generate the single level for maxZoom.
      * @param  srcCRS              CRS of the original geometry
      * @param  destCRS             (optional) The CRS to produce vectortiles into.  When omitted,
      *                             defaults to [[geotrellis.proj4.WebMercator WebMercator]].
      * @param  useCaching          Allows intermediate results to be cached to disk.  May require
      *                             additional disk space on executor nodes.
      * @param  orderAreas          Sorts polygonal geometries in vectortiles.  In case of overlaps,
      *                             smaller geometries will draw on top of larger ones.
      * @param  tileResolution      Resolution of output tiles; i.e., the number of discretized bins
      *                             (along each axis) to quantize coordinates to for display.
      */
    def apply(
      maxZoom: Int,
      minZoom: Option[Int],
      srcCRS: CRS,
      destCRS: Option[CRS] = None,
      useCaching: Boolean = false,
      orderAreas: Boolean = false,
      tileResolution: Int = 4096
    ): Options = {
      val destOrWebMercatorCRS = destCRS.getOrElse(WebMercator)
      val minOrMaxZoom = math.min(math.max(0, minZoom.getOrElse(maxZoom)), maxZoom)
      val zls = ZoomedLayoutScheme(destOrWebMercatorCRS, tileResolution)
      val layoutLevels = Range.Int(maxZoom, minOrMaxZoom, -1).inclusive.map(zls.levelForZoom)

      Options(layoutLevels, srcCRS, destOrWebMercatorCRS, useCaching, orderAreas)
    }

    def forZoom(zoom: Int) = Options(zoom, None, LatLng)
    def forZoomRange(minZoom: Int, maxZoom: Int) = Options(maxZoom, Some(minZoom), LatLng)
    def forAllZoomsFrom(zoom: Int) = Options(zoom, Some(0), LatLng)
    def forAllZoomsWithSrcProjection(zoom: Int, crs: CRS) = Options(zoom, Some(0), crs)
  }

  def apply(input: DataFrame, pipeline: vectortile.Pipeline, options: Options): Unit = {
    val geomColumn = pipeline.geometryColumn
    assert(input.columns.contains(geomColumn) &&
           input.schema(geomColumn).dataType.isInstanceOf[org.apache.spark.sql.jts.AbstractGeometryUDT[_]],
           s"Input DataFrame must contain a column `${geomColumn}` of JTS Geometry")
    assert(options.layoutLevels.map(_.layout.tileLayout).forall(layout => layout.tileCols == layout.tileRows),
           s"Input layoutLevels must define square tiles (tileCols != tileRows)")
    assert(options.layoutLevels.map(_.zoom).toSet.size == options.layoutLevels.size,
           s"Input layoutLevels cannot contain duplicate zoom levels")

    val srcCRS = options.srcCRS
    val destCRS = options.destCRS

    val layoutLevels = options.layoutLevels.sortBy(-_.zoom)
    val maxZoom = layoutLevels.head.zoom

    // Reproject geometries if needed
    val reprojected = if (srcCRS != destCRS) {
      input.withColumn(geomColumn, st_reprojectGeom(col(geomColumn), lit(srcCRS.toProj4String), lit(destCRS.toProj4String)))
    } else {
      input
    }

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
      val clip = udf { (g: Geometry, key: GenericRowWithSchema) =>
        val k = getSpatialKey(key)
        pipeline.clip(g, k, level)
      }

      val selectedGeometry = pipeline
        .select(df, zoom, keyColumn)

      val clipped = selectedGeometry
        .withColumn(keyColumn, explode(col(keyColumn)))
        .repartition(col(keyColumn)) // spread copies of possibly ill-tempered geometries around cluster prior to clipping
        .withColumn(geomColumn, clip(col(geomColumn), col(keyColumn)))

      val tileWidth = level.layout.tileLayout.layoutCols // equal to layoutRows

      pipeline.layerMultiplicity match {
        case SingleLayer(layerName) =>
          clipped
            .rdd
            .map { r => (getSpatialKey(r, keyColumn), pipeline.pack(r, zoom)) }
            .groupByKey
            .map { case (key, feats) =>
               val ex = level.layout.mapTransform.keyToExtent(key)
               key -> buildVectorTile(feats, layerName, ex, tileWidth, options.orderAreas)
            }
        case LayerNamesInColumn(layerNameCol) =>
          assert(selectedGeometry.schema(layerNameCol).dataType == StringType,
                 s"layerMultiplicity=${pipeline.layerMultiplicity} requires String-type column of name ${layerNameCol}")
          clipped
            .rdd
            .map { r => (getSpatialKey(r, keyColumn), r.getAs[String](layerNameCol) -> pipeline.pack(r, zoom)) }
            .groupByKey
            .mapPartitions{ iter: Iterator[(SpatialKey, Iterable[(String, VectorTileFeature[Geometry])])] =>
              iter.map{ case (key, groupedFeatures) => {
                val layerFeatures: Map[String, Iterable[VectorTileFeature[Geometry]]] =
                  groupedFeatures.groupBy(_._1).mapValues(_.map(_._2))
                val ex = level.layout.mapTransform.keyToExtent(key)
                key -> buildVectorTile(layerFeatures, ex, tileWidth, options.orderAreas)
              }}
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

    layoutLevels.foldLeft(reprojected){ (df, level) =>
      val working =
        if (level.zoom == maxZoom) {
          df.withColumn(keyColumn, keyTo(level.layout)(col(geomColumn)))
        } else {
          df
        }
      val simplify = udf { g: Geometry => pipeline.simplify(g, level.layout) }
      val reduced = pipeline
        .reduce(working, level, keyColumn)
      val persisted = if (options.useCaching) reduced.persist(StorageLevel.DISK_ONLY) else reduced
      val prepared = persisted
        .withColumn(geomColumn, simplify(col(geomColumn)))
      val vts = generateVectorTiles(prepared, level)
      saveVectorTiles(vts, level.zoom, pipeline.baseOutputURI)
      prepared.withColumn(keyColumn, reduceKeys(col(keyColumn)))
    }
  }

  /** Construct a SparkSession with defaults tailored for use with VectorPipe
   *  This session enables:
   *   - Hive support
   *   - JTS support
   *   - Kryo serialization
   *   - spark master local[*] if unset for local testing
   *
   *  If you need more control over config, you'll need to construct your own session
   */
  def defaultSparkSessionWithJTS(appName: String): SparkSession = {
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName(appName)
      .set("spark.sql.orc.impl", "native")
      .set("spark.sql.orc.filterPushdown", "true")
      .set("spark.sql.parquet.mergeSchema", "false")
      .set("spark.sql.parquet.filterPushdown", "true")
      .set("spark.sql.hive.metastorePartitionPruning", "true")
      .set("spark.ui.showConsoleProgress", "true")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    SparkSession.builder
      .config(conf)
      .enableHiveSupport
      .getOrCreate
      .withJTS
  }
}
