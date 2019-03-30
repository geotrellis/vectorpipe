package vectorpipe

import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.locationtech.jts.{geom => jts}

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}

package object vectortile {
  type VectorTileFeature[+G <: Geometry] = Feature[G, Map[String, Value]]

  val logger = org.apache.log4j.Logger.getRootLogger

  val st_reprojectGeom = udf { (g: jts.Geometry, srcProj: String, destProj: String) =>
    val gt = Geometry(g)
    val trans = Proj4Transform(CRS.fromString(srcProj), CRS.fromString(destProj))
    gt.reproject(trans).jtsGeom
  }

  // case class IdFeature[+G <: Geometry, +D](geom: Geometry, data: D, id: Int) extends Feature[G, D](geom, data) {
  //   override def mapGeom[T <: Geometry](f: G => T): IdFeature[T, D] =
  //     IdFeature(f(geom), data, id)

  //   override def mapData[T](f: D => T): IdFeature[G, T] =
  //     IdFeature(geom, f(data), id)
  // }

  def timedIntersect(geom: Geometry, ex: Extent, id: String)(implicit ec: ExecutionContext) = {
    val future = Future { geom.intersection(ex) }
    Try(Await.result(future, 5000 milliseconds)) match {
      case Success(res) => res
      case Failure(_) =>
        logger.warn(s"Could not intersect $geom with $ex [feature id=$id] in 5000 milliseconds")
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
      case mp: MultiPoint   => copy(multipoints=mp.asInstanceOf[VectorTileFeature[MultiPoint]] :: multipoints)
      case l : Line         => copy(lines=l.asInstanceOf[VectorTileFeature[Line]] :: lines)
      case ml: MultiLine    => copy(multilines=ml.asInstanceOf[VectorTileFeature[MultiLine]] :: multilines)
      case p : Polygon      => copy(polygons=p.asInstanceOf[VectorTileFeature[Polygon]] :: polygons)
      case mp: MultiPolygon => copy(multipolygons=mp.asInstanceOf[VectorTileFeature[MultiPolygon]] :: multipolygons)
    }
  }
  object VTContents {
    def empty() = VTContents()
  }

  def buildVectorTile[G <: Geometry](geoms: Iterable[VectorTileFeature[G]], ex: Extent, layerName: String, tileWidth: Int): VectorTile = {
    val contents = geoms.foldLeft(VTContents.empty){ (accum, feature) => accum + feature }
    val VTContents(pts, mpts, ls, mls, ps, mps) = contents
    val layer = StrictLayer(
      name=layerName,
      tileWidth=tileWidth,
      version=2,
      tileExtent=ex,
      points=pts,
      multiPoints=mpts,
      lines=ls,
      multiLines=mls,
      polygons=ps, //.sortWith(_.area > _.area),
      multiPolygons=mps //.sortWith(_.area > _.area)
    )

    VectorTile(Map(layerName -> layer), ex)
  }


}
