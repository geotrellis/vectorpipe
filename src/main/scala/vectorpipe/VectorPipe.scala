package vectorpipe

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import vectorpipe.vectortile._
import vectorpipe.vectortile.export._
import geotrellis.proj4.{CRS, LatLng, WebMercator}
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.{LayoutLevel, ZoomedLayoutScheme}
import geotrellis.vector.{Extent, Feature, Geometry}
import geotrellis.vectortile._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.locationtech.jts.{geom => jts}
import org.locationtech.geomesa.spark.jts.SparkSessionWithJTS

case class TiledFeature(sk: SpatialKey, layer: String, geom: jts.Geometry, data: Map[String, Array[Byte]]) {
  def feature: VectorTileFeature[Geometry] = Feature(geom, data.mapValues {
    b =>
      val bb = ByteBuffer.wrap(b)
      bb.get() match {
        case 0 =>
          val bytes = Array.ofDim[Byte](bb.remaining())
          bb.get(bytes)
          VString(new String(bytes, StandardCharsets.UTF_8))
        case 1 => VFloat(bb.getFloat())
        case 2 => VDouble(bb.getDouble())
        case 3 => VInt64(bb.getLong())
        case 4 => VWord64(bb.getLong())
        case 5 => VSint64(bb.getLong())
        case 6 => VBool(bb.get() == 1)
      }
  })
}

object TiledFeature {
  def apply(sk: SpatialKey, layer: String, feature: VectorTileFeature[Geometry]): TiledFeature =
    TiledFeature(sk, layer, feature.geom.jtsGeom, feature.data.mapValues {
      case VString(v) => (0: Byte) +: v.getBytes(StandardCharsets.UTF_8)
      case VFloat(v) =>
        ByteBuffer.allocate(5).put(2: Byte).putFloat(v).array()
      case VDouble(v) =>
        ByteBuffer.allocate(9).put(2: Byte).putDouble(v).array()
      case VInt64(v) =>
        ByteBuffer.allocate(9).put(3: Byte).putLong(v).array()
      case VWord64(v) =>
        ByteBuffer.allocate(9).put(4: Byte).putLong(v).array()
      case VSint64(v) =>
        ByteBuffer.allocate(9).put(5: Byte).putLong(v).array()
      case VBool(v) if v =>
        Array(6, 1)
      case VBool(_) =>
        Array(6, 0)
    })
}

case class Tile(sk: SpatialKey, bytes: Array[Byte], extent: Extent) {
  def tile: VectorTile = VectorTile.fromBytes(bytes, extent)
}

object Tile {
  def apply(sk: SpatialKey, tile: VectorTile): Tile = Tile(sk, tile.toBytes, tile.tileExtent)
}

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

    def generateVectorTiles(df: DataFrame, level: LayoutLevel): Dataset[Tile] = {
      import df.sparkSession.implicits._
      df.sparkSession.withJTS

      val zoom = level.zoom
      val clip = udf { (g: jts.Geometry, key: GenericRowWithSchema) =>
        val k = getSpatialKey(key)
        pipeline.clip(g, k, level)
      }

      val selectedGeometry = pipeline
        .select(df, zoom, keyColumn)
        .withColumn(keyColumn, explode(col(keyColumn)))
        .repartition(col(keyColumn)) // spread copies of possibly ill-tempered geometries around cluster prior to clipping

      val clipped = pipeline.layerMultiplicity match {
        case SingleLayer(layerName) =>
          selectedGeometry
            .withColumn(geomColumn, clip(col(geomColumn), col(keyColumn)))
            .map {
              row =>
                TiledFeature(getSpatialKey(row, keyColumn), layerName, pipeline.pack(row, zoom))
            }
        case LayerNamesInColumn(layerNameCol) =>
          assert(selectedGeometry.schema(layerNameCol).dataType == StringType,
            s"layerMultiplicity=${pipeline.layerMultiplicity} requires String-type column of name ${layerNameCol}")

          selectedGeometry
            .withColumn(geomColumn, clip(col(geomColumn), col(keyColumn)))
            .map {
              row =>
                TiledFeature(getSpatialKey(row, keyColumn), row.getAs[String](layerNameCol), pipeline.pack(row, zoom))
            }
      }

      clipped
        .groupByKey(r => r.sk)
        .mapGroups {
          case (sk, groupedFeatures) =>
            val layerFeatures: Map[String, Iterable[VectorTileFeature[Geometry]]] =
              groupedFeatures.toIterable.groupBy(_.layer).mapValues(_.map(_.feature))
            val extent = level.layout.mapTransform.keyToExtent(sk)
            Tile(sk, buildVectorTile(layerFeatures, extent, options.tileResolution, options.orderAreas))
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
        .localCheckpoint()
      val prepared = reduced
        .withColumn(geomColumn, simplify(col(geomColumn)))
      val vts = generateVectorTiles(prepared, level)
      saveVectorTiles(vts, zoom, pipeline.baseOutputURI)
      prepared.withColumn(keyColumn, reduceKeys(col(keyColumn)))
    }

  }

}
