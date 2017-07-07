---
layout: docs
title: "Concepts"
section: "usage"
---

# Concepts

VectorPipe strives to be straight-forward. With only a few simple function
applications we can transform completely raw data into a grid of
VectorTiles, ready for further processing. "Clipping" and "Collation"
functions help us customize this process along the way.

![](/img/pipeline.png)

### Data Sources

Some source of Vector (re: geometric) data on the earth. Could come in any
format (example: OpenStreetMap).

For each data source that has first-class support, we expose a
`vectorpipe.*` module with a matching name. Example: `vectorpipe.osm`. These
modules expose all the types and functions necessary for transforming the
raw data into the "Middle Ground" types.

No first-class support for your favourite data source? Want to write it
yourself, and maybe even keep it private? That's okay, just provide the
function `YourData => RDD[Feature[G, D]]` and VectorPipe can handle the
rest.

### The "Middle Ground"

A collection of Geometries on the earth. The actual data can be distributed
across multiple machines via Spark's `RDD` type. From this "middle ground",
we can proceed with creating Vector Tiles, or (with the right supporting
code) we could convert *back* into the format of the original source data.

Note that via the method `VectorTile.toIterator`, the following conversion
is possible:

```tut:silent
import geotrellis.spark._
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark._
import org.apache.spark.rdd.RDD

implicit val sc: SparkContext = new SparkContext(
  new SparkConf().setMaster("local[*]").setAppName("back-to-middle-ground")
)

/* Mocked as `empty` for the example */
val tiles: RDD[(SpatialKey, VectorTile)] = sc.emptyRDD

/* A VT layer converted back to the "middle ground", possibly for recollation */
val backToMiddle: RDD[(SpatialKey, Iterator[Feature[Geometry, Map[String, Value]]])] =
  tiles.mapValues(_.toIterator)

/* Close up Spark nicely */
sc.stop()
```

### Clipping Functions

GeoTrellis has a consistent `RDD[(K, V)]` pattern for handling grids of
tiled data, where `K` is the grid index and `V` is the actual value type.
Before `RDD[(SpatialKey, VectorTile)]` can be achieved, we need to convert
our gridless `RDD[Feature[G, D]]` into such a grid, such that each Feature's
`Geometry` is reasonably clipped to the size of an individual tile. Depending
on which clipping function you choose (from the `vectorpipe.Clip` object, or
even your own custom one) the shape of the clipped Geometry will vary. See
our Scaladocs for more detail on the available options.

### Collation Functions

Once clipped and gridded by `VectorPipe.toGrid`, we have a `RDD[(SpatialKey,
Iterable[Feature[G, D]])]` that represents all the Geometry fragments
present at each tiled location on the earth. This is the perfect shape to
turn into a `VectorTile`. To do so, we need to choose a *Collator* function,
which determines what VectorTile Layer each `Feature` should be placed into,
and how (if at all) its corresponding metadata (the `D`) should be
processed.

Want to write your own Collator? The `Collate.generically` function will be
of interest to you.

### Output Targets

We can imagine two possible outputs for our completed grid of Vector Tiles:

- A compressed GeoTrellis layer, saved to S3 [or
elsewhere](https://geotrellis.readthedocs.io/en/latest/guide/tile-backends.html)
- A dump of every tile as an `.mvt`, readable by other software

Either option is simple, but outputting an `RDD[(SpatialKey, VectorTile)]`
isn't actually the concern of VectorPipe - it can be handled entirely in
client code via GeoTrellis functionality. An example of this can be found
[in this repository](https://github.com/fosskers/vectorpipe-io).
