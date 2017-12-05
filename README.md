VectorPipe
==========

[![Bintray](https://img.shields.io/bintray/v/azavea/maven/vectorpipe.svg)](https://bintray.com/azavea/maven/vectorpipe)

A pipeline for mass conversion of Vector data (OpenStreetMap, etc.) into
Mapbox VectorTiles. Powered by [GeoTrellis](http://geotrellis.io) and
[Apache Spark](http://spark.apache.org/).

Overview
--------

![](docs/pipeline.png)

There are four main stages here which represent the four main shapes of
data in the pipeline:

1. Unprocessed Vector (geometric) Data
2. Clipped GeoTrellis `Feature`s organized into a Grid on the earth
3. VectorTiles organized into a Grid on the earth
4. Fully processed VectorTiles, output to some target

Of these, Stage 4 is left to the user to leverage GeoTrellis directly in
their own application. Luckily the `RDD[(SpatialKey, VectorTile)] => Unit`
operation only requires about 5 lines of code. Stages 1 to 3 then are the
primary concern of Vectorpipe.

### Processing Raw Data

For each data source that has first-class support, we expose a
`vectorpipe.*` module with a matching name. Example: `vectorpipe.osm`. These
modules expose all the types and functions necessary for transforming the
raw data into the "Middle Ground" types.

No first-class support for your favourite data source? Want to write it
yourself, and maybe even keep it private? That's okay, just provide the
function `YourData => RDD[Feature[G, D]]` and VectorPipe can handle the
rest.

### Clipping Features into a Grid

GeoTrellis has a consistent `RDD[(K, V)]` pattern for handling grids of
tiled data, where `K` is the grid index and `V` is the actual value type.
Before `RDD[(SpatialKey, VectorTile)]` can be achieved, we need to convert
our gridless `RDD[Feature[G, D]]` into such a grid, such that each Feature's
`Geometry` is reasonably clipped to the size of an individual tile. Depending
on which clipping function you choose (from the `vectorpipe.Clip` object, or
even your own custom one) the shape of the clipped Geometry will vary. See
our Scaladocs for more detail on the available options.

### Collating Feature Groups into a VectorTile

Once clipped and gridded by `VectorPipe.toGrid`, we have a `RDD[(SpatialKey,
Iterable[Feature[G, D]])]` that represents all the Geometry fragments
present at each tiled location on the earth. This is the perfect shape to
turn into a `VectorTile`. To do so, we need to choose a *Collator* function,
which determines what VectorTile Layer each `Feature` should be placed into,
and how (if at all) its corresponding metadata (the `D`) should be
processed.

Want to write your own Collator? The `Collate.generically` function will be
of interest to you.
