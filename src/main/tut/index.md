---
layout: home
title: "Home"
section: "section_home"
position: 1
technologies:
 - first: ["GeoTrellis", "Geographic data processing engine for high performance applications"]
 - second: ["Apache Spark", "An engine for large-scale data processing"]
 - third: ["Scala", "Functional Programming on the JVM"]
---

**Note: VectorPipe has not yet been released.**

# VectorPipe

VectorPipe is a Scala library for transforming vector data of arbitrary
sources into [Mapbox Vector Tiles](https://www.mapbox.com/vector-tiles/). It
uses the VectorTile codec from the [GeoTrellis library
suite](https://geotrellis.io/), which in turn is powered by [Apache
Spark](https://spark.apache.org/).

Currently VectorPipe can process:

- OpenStreetMap XML / PBF* / ORC

And produce:

- Analytic Vector Tiles (AVTs)
- Custom Vector Tile schemes (by writing a custom *Collator* function)

### Dependencies

- Scala 2.11
- Apache Spark 2.1.0+

### Getting Started

To use VectorPipe, add the following to your `build.sbt`:

```
libraryDependencies += "com.azavea" %% "vectorpipe" % "1.0.0"
```

Now import the following, and you're good to go:

```tut:silent
import vectorpipe._
```

### Performance

Wow, fast!

### Related Projects

- [OpenMapTiles](https://openmaptiles.org/)
- [Mapbox](https://www.mapbox.com/)
