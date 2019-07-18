package vectorpipe

import geotrellis.proj4._
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.locationtech.jts.{geom => jts}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

package object vectortile {
  type VectorTileFeature[+G <: Geometry] = Feature[G, Map[String, Value]]

  sealed trait LayerMultiplicity { val name: String }
  case class SingleLayer(val name: String) extends LayerMultiplicity
  case class LayerNamesInColumn(val name: String) extends LayerMultiplicity

  @transient lazy val logger = org.apache.log4j.Logger.getRootLogger

  @transient lazy val st_reprojectGeom = udf { (g: jts.Geometry, srcProj: String, destProj: String) =>
    val trans = Proj4Transform(CRS.fromString(srcProj), CRS.fromString(destProj))
    if (Option(g).isDefined) {
      if (g.isEmpty)
        g
      else {
        val gt = Geometry(g)
        gt.reproject(trans).jtsGeom
      }
    } else {
      null
    }
  }

  def keyTo(layout: LayoutDefinition) = udf { g: jts.Geometry =>
    if (Option(g).isDefined) {
      layout.mapTransform.keysForGeometry(geotrellis.vector.Geometry(g)).toArray
    } else {
      Array.empty[SpatialKey]
    }
  }

  def getSpatialKey(k: GenericRowWithSchema): SpatialKey = SpatialKey(k.getInt(0), k.getInt(1))

  def getSpatialKey(row: Row, field: String): SpatialKey = {
    val k = row.getAs[Row](field)
    SpatialKey(k.getInt(0), k.getInt(1))
  }

  // case class IdFeature[+G <: Geometry, +D](geom: Geometry, data: D, id: Int) extends Feature[G, D](geom, data) {
  //   override def mapGeom[T <: Geometry](f: G => T): IdFeature[T, D] =
  //     IdFeature(f(geom), data, id)

  //   override def mapData[T](f: D => T): IdFeature[G, T] =
  //     IdFeature(geom, f(data), id)
  // }

  def timedIntersect[G <: Geometry](geom: G, ex: Extent)(implicit ec: ExecutionContext) = {
    val future = Future { geom.intersection(ex) }
    Try(Await.result(future, 5000 milliseconds)) match {
      case Success(res) => res
      case Failure(_) =>
        logger.warn(s"Could not intersect $geom with $ex in 5000 milliseconds")
        NoResult
    }
  }

  case class VTContents(points: List[VectorTileFeature[Point]] = Nil,
                        multipoints: List[VectorTileFeature[MultiPoint]] = Nil,
                        lines: List[VectorTileFeature[Line]] = Nil,
                        multilines: List[VectorTileFeature[MultiLine]] = Nil,
                        polygons: List[VectorTileFeature[Polygon]] = Nil,
                        multipolygons: List[VectorTileFeature[MultiPolygon]] = Nil) {
    def +(other: VTContents) = VTContents(points ++ other.points,
                                          multipoints ++ other.multipoints,
                                          lines ++ other.lines,
                                          multilines ++ other.multilines,
                                          polygons ++ other.polygons,
                                          multipolygons ++ other.multipolygons)
    def +[G <: Geometry](other: VectorTileFeature[G]) = other.geom match {
      case p : Point        => copy(points=other.asInstanceOf[VectorTileFeature[Point]] :: points)
      case mp: MultiPoint   => copy(multipoints=other.asInstanceOf[VectorTileFeature[MultiPoint]] :: multipoints)
      case l : Line         => copy(lines=other.asInstanceOf[VectorTileFeature[Line]] :: lines)
      case ml: MultiLine    => copy(multilines=other.asInstanceOf[VectorTileFeature[MultiLine]] :: multilines)
      case p : Polygon      => copy(polygons=other.asInstanceOf[VectorTileFeature[Polygon]] :: polygons)
      case mp: MultiPolygon => copy(multipolygons=other.asInstanceOf[VectorTileFeature[MultiPolygon]] :: multipolygons)
    }
  }
  object VTContents {
    def empty() = VTContents()
  }

  def buildLayer[G <: Geometry](features: Iterable[VectorTileFeature[G]], layerName: String, ex: Extent, tileWidth: Int): Layer = {
    val contents = features.foldLeft(VTContents.empty){ (accum, feature) => accum + feature }
    val VTContents(pts, mpts, ls, mls, ps, mps) = contents
    StrictLayer(
      name=layerName,
      tileWidth=tileWidth,
      version=2,
      tileExtent=ex,
      points=pts,
      multiPoints=mpts,
      lines=ls,
      multiLines=mls,
      polygons=ps,
      multiPolygons=mps
    )
  }

  def buildSortedLayer[G <: Geometry](features: Iterable[VectorTileFeature[G]], layerName: String, ex: Extent, tileWidth: Int): Layer = {
    val contents = features.foldLeft(VTContents.empty){ (accum, feature) => accum + feature }
    val VTContents(pts, mpts, ls, mls, ps, mps) = contents
    StrictLayer(
      name=layerName,
      tileWidth=tileWidth,
      version=2,
      tileExtent=ex,
      points=pts,
      multiPoints=mpts,
      lines=ls,
      multiLines=mls,
      polygons=ps.sortWith(_.area > _.area),
      multiPolygons=mps.sortWith(_.area > _.area)
    )
  }

  def buildVectorTile[G <: Geometry](
    features: Iterable[VectorTileFeature[G]],
    layerName: String,
    ex: Extent,
    tileWidth: Int,
    sorted: Boolean
  ): VectorTile = {
    val layer =
      if (sorted)
        buildSortedLayer(features, layerName, ex, tileWidth)
      else
        buildLayer(features, layerName, ex, tileWidth)
    VectorTile(Map(layerName -> layer), ex)
  }

  def buildVectorTile[G <: Geometry](
    layerFeatures: Map[String, Iterable[VectorTileFeature[G]]],
    ex: Extent,
    tileWidth: Int,
    sorted: Boolean
  ): VectorTile = {
    VectorTile(layerFeatures.map{ case (layerName, features) => (layerName,
      if (sorted)
        buildSortedLayer(features, layerName, ex, tileWidth)
      else
        buildLayer(features, layerName, ex, tileWidth))
    }, ex)
  }

}
