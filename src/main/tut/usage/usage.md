# Usage

Writing a small executable that uses VectorPipe is straight-forward. The
entire `main` isn't much more than:

```tut:silent
import geotrellis.proj4.WebMercator
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vectortile.VectorTile
import org.apache.spark._
import org.apache.spark.rdd.RDD
import vectorpipe._  /* All types and functions. Also exposes the `osm` submodule used below. */

/* Initialize a `SparkContext`, necessary for all `RDD` work */
implicit val sc: SparkContext = new SparkContext(
  new SparkConf().setMaster("local[*]").setAppName("vectorpipe-example")
)

/* Describe the dimensions of your data area */
val layout: LayoutDefinition =
  ZoomedLayoutScheme.layoutForZoom(15, WebMercator.worldExtent, 512)

/* From an OSM data source, mocked as "empty" for this example */
val (nodes, ways, relations): (RDD[osm.Node], RDD[osm.Way], RDD[osm.Relation]) =
  (sc.emptyRDD, sc.emptyRDD, sc.emptyRDD)

/* All OSM Elements lifted into GeoTrellis Geometry types.
 * Note: type OSMFeature = Feature[Geometry, Tree[ElementData]]
 */
val features: RDD[osm.OSMFeature] =
  osm.toFeatures(nodes, ways, relations)

/* All Geometries clipped to your `layout` grid */
val featGrid: RDD[(SpatialKey, Iterable[osm.OSMFeature])] =
  VectorPipe.toGrid(Clip.byHybrid, osm.stdout, layout, features)

/* A grid of Vector Tiles */
val tiles: RDD[(SpatialKey, VectorTile)] =
  VectorPipe.toVectorTile(Collate.byAnalyticsLite, layout, featGrid)

/* Further processing here, writing to S3, etc. */

/* Halt Spark nicely */
sc.stop()
```

A full example of processing some OSM XML [can be found
here](https://github.com/fosskers/vectorpipe-io).
