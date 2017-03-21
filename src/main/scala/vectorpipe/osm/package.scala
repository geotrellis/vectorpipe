package vectorpipe

import geotrellis.proj4.CRS
import geotrellis.vector._
import vectorpipe.util.Tree

// --- //

package object osm {

  type TagMap = Map[String, String]
  type OSMFeature = Feature[Geometry, (Tree[ElementData], Extent)]
  type OSMPoint = Feature[Point, Tree[ElementData]]
  type OSMLine = Feature[Line, Tree[ElementData]]
  type OSMPolygon = Feature[Polygon, Tree[ElementData]]
  type OSMMultiPoly = Feature[MultiPolygon, Tree[ElementData]]

  /** Reproject an [[OSMFeature]]. */
  def reproject(f: OSMFeature, src: CRS, dest: CRS): OSMFeature = {
    val (tree, extent) = f.data

    f.reproject(src, dest).copy(data = (tree, extent.reproject(src, dest)))
  }

}
