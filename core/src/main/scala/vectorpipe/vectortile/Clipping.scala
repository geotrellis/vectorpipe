package vectorpipe.vectortile

import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutLevel
import geotrellis.vector._
import org.locationtech.jts.{geom => jts}

import scala.concurrent.ExecutionContext.Implicits.global

object Clipping {
    def byLayoutCell(geom: jts.Geometry, key: SpatialKey, layoutLevel: LayoutLevel): jts.Geometry = {
      val gtg = Geometry(geom)
      val ex = layoutLevel.layout.mapTransform.keyToExtent(key)

      // Preserve dimension of resultant geometry
      val clipped = gtg match {
        case p: Point => p.jtsGeom  // points with the current key intersect the extent by definition
        case mp: MultiPoint =>
          timedIntersect(mp, ex) match {
            case PointResult(pr) => pr.jtsGeom
            case MultiPointResult(mpr) => mpr.jtsGeom
            case NoResult =>
              logger.warn(s"$gtg was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => // should never match here; just shut the compiler up
              geom
          }
        case l: Line =>
          timedIntersect(l, ex) match {
            case LineResult(lr) => lr.jtsGeom
            case MultiLineResult(mlr) => mlr.jtsGeom
            case GeometryCollectionResult(gcr) =>
              gcr.lines.length match {
                case 0 => MultiLine().jtsGeom
                case 1 => gcr.lines(0).jtsGeom
                case _ => MultiLine(gcr.lines).jtsGeom
              }
            case NoResult =>
              logger.warn(s"$gtg was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ =>
              MultiLine().jtsGeom // Discard (multi-)point results
          }
        case ml: MultiLine =>
          timedIntersect(ml, ex) match {
            case LineResult(lr) => lr.jtsGeom
            case MultiLineResult(mlr) => mlr.jtsGeom
            case GeometryCollectionResult(gcr) =>
              (gcr.lines.length, gcr.multiLines.length) match {
                case (0, 0) => MultiLine().jtsGeom
                case (1, 0) => gcr.lines(0).jtsGeom
                case (0, 1) => gcr.multiLines(0).jtsGeom
                case _ => MultiLine(gcr.lines ++ gcr.multiLines.flatMap(_.lines.toSeq)).jtsGeom
              }
            case NoResult =>
              logger.warn(s"$gtg was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ =>
              MultiLine().jtsGeom // Discard (multi-)point results
          }
        case poly: Polygon =>
          timedIntersect(poly, ex) match {
            case PolygonResult(pr) => pr.jtsGeom
            case MultiPolygonResult(mpr) => mpr.jtsGeom
            case GeometryCollectionResult(gcr) =>
              gcr.polygons.length match {
                case 0 => MultiPolygon().jtsGeom
                case 1 => gcr.polygons(0).jtsGeom
                case _ => MultiPolygon(gcr.polygons).jtsGeom
              }
            case NoResult =>
              logger.warn(s"$gtg was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => MultiPolygon().jtsGeom // ignore point/line results
          }
        case mp: MultiPolygon =>
          timedIntersect(mp, ex) match {
            case PolygonResult(pr) => pr.jtsGeom
            case MultiPolygonResult(mpr) => mpr.jtsGeom
            case GeometryCollectionResult(gcr) =>
              (gcr.polygons.length, gcr.multiPolygons.length) match {
                case (0, 0) => MultiPolygon().jtsGeom
                case (1, 0) => gcr.polygons(0).jtsGeom
                case (0, 1) => gcr.multiPolygons(0).jtsGeom
                case _ => MultiPolygon(gcr.polygons ++ gcr.multiPolygons.flatMap(_.polygons.toSeq)).jtsGeom
              }
            case NoResult =>
              logger.warn(s"$gtg was keyed to layout cell $key, but did not intersect $ex [zoom=${layoutLevel.zoom}]")
              geom
            case _ => MultiPolygon().jtsGeom // ignore point/line results
          }
      }
      clipped
    }

}
