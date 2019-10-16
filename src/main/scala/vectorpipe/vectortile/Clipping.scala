package vectorpipe.vectortile

import geotrellis.layer.SpatialKey
import geotrellis.layer.LayoutLevel
import geotrellis.vector._

import scala.concurrent.ExecutionContext.Implicits.global

object Clipping {
    def byLayoutCell(geom: Geometry, key: SpatialKey, layoutLevel: LayoutLevel): Geometry = {
      val ex = layoutLevel.layout.mapTransform.keyToExtent(key)

      // Preserve dimension of resultant geometry
      val clipped = geom match {
        case p: Point => p // points with the current key intersect the extent by definition
        case mp: MultiPoint =>
          timedIntersect(mp, ex) match {
            case PointResult(pr) => pr
            case MultiPointResult(mpr) => mpr
            case NoResult =>
              logger.warn(s"$geom was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => // should never match here; just shut the compiler up
              geom
          }
        case l: LineString =>
          timedIntersect(l, ex) match {
            case LineStringResult(lr) => lr
            case MultiLineStringResult(mlr) => mlr
            case GeometryCollectionResult(gcr) =>
              gcr.getAll[LineString].length match {
                case 0 => MultiLineString()
                case 1 => gcr.getAll[LineString].head
                case _ => MultiLineString(gcr.getAll[LineString])
              }
            case NoResult =>
              logger.warn(s"$geom was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ =>
              MultiLineString() // Discard (multi-)point results
          }
        case ml: MultiLineString =>
          timedIntersect(ml, ex) match {
            case LineStringResult(lr) => lr
            case MultiLineStringResult(mlr) => mlr
            case GeometryCollectionResult(gcr) =>
              (gcr.getAll[LineString].length, gcr.getAll[MultiLineString].length) match {
                case (0, 0) => MultiLineString()
                case (1, 0) => gcr.getAll[LineString].head
                case (0, 1) => gcr.getAll[MultiLineString].head
                case _ => MultiLineString(gcr.getAll[LineString] ++ gcr.getAll[MultiLineString].flatMap(_.lines.toSeq))
              }
            case NoResult =>
              logger.warn(s"$geom was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ =>
              MultiLineString() // Discard (multi-)point results
          }
        case poly: Polygon =>
          timedIntersect(poly, ex) match {
            case PolygonResult(pr) => pr
            case MultiPolygonResult(mpr) => mpr
            case GeometryCollectionResult(gcr) =>
              gcr.getAll[Polygon].length match {
                case 0 => MultiPolygon()
                case 1 => gcr.getAll[Polygon].head
                case _ => MultiPolygon(gcr.getAll[Polygon])
              }
            case NoResult =>
              logger.warn(s"$geom was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => MultiPolygon() // ignore point/line results
          }
        case mp: MultiPolygon =>
          timedIntersect(mp, ex) match {
            case PolygonResult(pr) => pr
            case MultiPolygonResult(mpr) => mpr
            case GeometryCollectionResult(gcr) =>
              (gcr.getAll[Polygon].length, gcr.getAll[MultiPolygon].length) match {
                case (0, 0) => MultiPolygon()
                case (1, 0) => gcr.getAll[Polygon].head
                case (0, 1) => gcr.getAll[MultiPolygon].head
                case _ => MultiPolygon(gcr.getAll[Polygon] ++ gcr.getAll[MultiPolygon].flatMap(_.polygons.toSeq))
              }
            case NoResult =>
              logger.warn(s"$geom was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => MultiPolygon() // ignore point/line results
          }
      }
      clipped
    }

}
