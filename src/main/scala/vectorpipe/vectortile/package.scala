package vectorpipe

import geotrellis.proj4._
import geotrellis.layer.SpatialKey
import geotrellis.layer.LayoutDefinition
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

package object vectortile {
  type VectorTileFeature[+G <: Geometry] = Feature[G, Map[String, Value]]

  def vtf2mvtf[G <: Geometry](vtf: VectorTileFeature[G]): MVTFeature[G] =
    MVTFeature(vtf.geom, vtf.data)

  sealed trait LayerMultiplicity { val name: String }
  case class SingleLayer(val name: String) extends LayerMultiplicity
  case class LayerNamesInColumn(val name: String) extends LayerMultiplicity

  @transient lazy val logger = org.apache.log4j.Logger.getRootLogger

  @transient lazy val st_reprojectGeom = udf { (g: Geometry, srcProj: String, destProj: String) =>
    val trans = Proj4Transform(CRS.fromString(srcProj), CRS.fromString(destProj))
    if (Option(g).isDefined) {
      if (g.isEmpty)
        g
      else {
        g.reproject(trans)
      }
    } else {
      null
    }
  }

  def keyTo(layout: LayoutDefinition) = udf { g: Geometry =>
    if (Option(g).isDefined && !g.isEmpty) {
      layout.mapTransform.keysForGeometry(g).toArray
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
    val future = Future { geom.&(ex) }
    Try(Await.result(future, 5000 milliseconds)) match {
      case Success(res) => res
      case Failure(_) =>
        logger.warn(s"Could not intersect $geom with $ex in 5000 milliseconds")
        NoResult
    }
  }

  case class VTContents(points: List[VectorTileFeature[Point]] = Nil,
                        multipoints: List[VectorTileFeature[MultiPoint]] = Nil,
                        lines: List[VectorTileFeature[LineString]] = Nil,
                        multilines: List[VectorTileFeature[MultiLineString]] = Nil,
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
      case l : LineString         => copy(lines=other.asInstanceOf[VectorTileFeature[LineString]] :: lines)
      case ml: MultiLineString    => copy(multilines=other.asInstanceOf[VectorTileFeature[MultiLineString]] :: multilines)
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
      points=pts.map(vtf2mvtf),
      multiPoints=mpts.map(vtf2mvtf),
      lines=ls.map(vtf2mvtf),
      multiLines=mls.map(vtf2mvtf),
      polygons=ps.map(vtf2mvtf),
      multiPolygons=mps.map(vtf2mvtf)
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
      points=pts.map(vtf2mvtf),
      multiPoints=mpts.map(vtf2mvtf),
      lines=ls.map(vtf2mvtf),
      multiLines=mls.map(vtf2mvtf),
      polygons=ps.sortWith(_.getArea > _.getArea).map(vtf2mvtf),
      multiPolygons=mps.sortWith(_.getArea > _.getArea).map(vtf2mvtf)
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
