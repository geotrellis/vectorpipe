VectorTiles for All
===================

Azavea is pursuing the development of a free-and-open-source pipeline for
mass conversion of [OpenStreetMap](https://www.openstreetmap.org/) data to
[Mapbox VectorTiles](https://www.mapbox.com/vector-tiles/). The project is
called [VectorPipe](https://github.com/geotrellis/vectorpipe) and is based
on [GeoTrellis](http://geotrellis.io), a Scala library also developed by
Azavea.

This post outlines our general approach, our current status, and some of the
roadbumps we've hit along the way.

The Six-fold Path
-----------------

OSM Planet data is freely available as a single, ~50gb XML file. In converting this
to a usable set of VectorTiles, we indentified six key steps:

1. [Fetch and preprocess the XML onto HDFS/S3](https://github.com/geotrellis/vectorpipe/issues/1)
  - Potentially splitting the XML into tens-of-thousands of smaller parts, to leverage
    Spark more effectively later
2. [Parse the XML into an `RDD[Element]`](https://github.com/geotrellis/vectorpipe/issues/2)
  - *Streaming* parsing to avoid blowing the JVM heap
3. [Convert the `Element`s to GeoTrellis `Feature`s](https://github.com/geotrellis/vectorpipe/issues/3)
  - Lots of "`RDD` algebra", as I've begun calling it
4. [Cut the `Feature`s into a grid](https://github.com/geotrellis/vectorpipe/issues/4)
5. [Convert the grid of `Feature`s to that of `VectorTile`s](https://github.com/geotrellis/vectorpipe/issues/5)
6. [Output the VectorTiles](https://github.com/locationtech/geotrellis/issues/1662)

Code from steps 2 and 3 is being tested, and step 6 was already possible
with our [VectorTile codec
implementation](https://geotrellis.github.io/scaladocs/latest/#geotrellis.vectortile.package).
Steps 1, 4, and 5 remain untouched at the moment.

In terms of the actual pipeline code, assuming Step 1 to have been
performed, the rest is as simple as:

```scala
import vectorpipe.osm._

/* Visible in any application using Spark */
implicit val sc: SparkContext = ...

/* How should a `SpatialKey` map to a filepath on S3? */
val s3PathFromKey: SpatialKey => String = SaveToS3.spatialKeyToPath(
  LayerId("sample", 1),
  "s3://some-bucket/catalog/{name}/{z}/{x}/{y}.mvt"
)

/* List of files containing OSM XML */
val files: Seq[String] = ...

/* The work */
sc.parallelize(files, numSlices = files.length)                   // RDD[String]
  .map(f => Element.parseFile(f))                                 // RDD[Element]
  .toFeatures                                                     // RDD[Feature]
  .toGrid                                                         // RDD[(SpatialKey, Seq[Feature])]
  .mapWithKey({ case (k, fs) => VectorTile.fromFeatures(k, fs) }) // RDD[(SpatialKey, VectorTile)]
  .mapValues(_.toBytes)                                           // RDD[(SpatialKey, Array[Byte])]
  .saveToS3(s3PathFromKey)                                        // Unit
```

where your final output is an S3 bucket of VectorTiles spanning the entire
earth, all for only a few dollars worth of AWS time.

Now, let's delve a bit deeper.

OpenStreetMap XML
-----------------

This section acts as a primer and can be skipped if you know how OSM
categorises their data.

OpenStreetMap has three primatives, collectively known as Elements. All
Elements have common attributes, and can contain further metadata in
`<tag>`s.

- `Node`
  - Any discrete location on earth, or a "node" in a Way
  - Translates easily to a GeoTrellis `Point` if not part of a Way
- `Way`
  - Some line-like object on the earth, like a road
  - If a Way is "closed", it translates nicely to a GeoTrellis `Polygon` and could represent a building
  - If "open", it becomes a `Line` if also not part of a "Geometric Relation"
- `Relation`
  - Any abstract grouping of Elements
  - Each Relation has a `type` tag, which can be set to anything
  - Some common types are geometric - like `multipolygon` or `boundary` - but these aren't set in stone
  - Multipolygon Relations have agreed-upon semantics for combining many Ways to form large
    Polygonal structures, like
    [enclaves and exclaves](https://en.wikipedia.org/wiki/Enclave_and_exclave)
    between countries
  - Relations can refer to other Relations, forming Relation Graphs
  - Relations store metadata relavent to the entire structure

Visual examples:

```xml
<node id="300006332" visible="true" version="2" changeset="11137522" timestamp="2012-03-29T12:06:03Z" user="Divjo" uid="63375" lat="24.3102749" lon="68.8355605"/>

<way id="19551824" visible="true" version="1" changeset="512823" timestamp="2008-01-02T12:20:13Z" user="dmgroom_coastlines" uid="4772">
  <nd ref="202991342"/>
  <nd ref="202993872"/>
  <nd ref="202993694"/>
  <nd ref="202993703"/>
  ...
  <tag k="admin_level" v="2"/>
  <tag k="border_type" v="nation"/>
  <tag k="boundary" v="administrative"/>
  ...
</way>

<relation id="549971" visible="true" version="2" changeset="4379439" timestamp="2010-04-10T05:58:16Z" user="Marcott" uid="173927">
  <member type="way" ref="54928207" role="outer"/>
  <member type="way" ref="54928404" role="inner"/>
  <tag k="type" v="multipolygon"/>
</relation>
```

Parsing XML
-----------

We opted to write our own parser for OSM XML because streaming parsing was a
priority, and we wanted control over the output types to better match Step 3
code. We settled on the [xml-spac](https://github.com/dylemma/xml-spac)
library for XML parsing, as it:

- streams!
- uses the Applicative combinator style, a la `scala-parser-combinators` and
its inspiration [parsec](https://hackage.haskell.org/package/parsec)

The surface area of the Scala dependency management world is quite large. We
have multiple build tools with their own ecosystems (try
[coursier](https://github.com/alexarchambault/coursier) if you use sbt) but
none help with actual package *discovery*. `xml-spac`'s author asked me how
I found it, to which the answer was "Google".
[Scaladex](https://index.scala-lang.org/) is a decent attempt at addressing
this, but isn't always up to date.

Writing composable parsers with `xml-spac` is easy, thanks to combinator
style:

```scala
implicit val elementMeta: Parser[Any, ElementMeta] = ... // too long for this example

/* <tag k='access' v='permissive' /> */
implicit val tag: Parser[Any, (String, String)] = (
  Parser.forMandatoryAttribute("k") ~ Parser.forMandatoryAttribute("v")
).as({ case (k,v) => (k,v) }) // Hand-holding the typesystem.

implicit val elementData: Parser[Any, ElementData] = (
  elementMeta ~ Splitter(* \ "tag").asListOf[(String, String)].map(_.toMap)
).as(ElementData)

/* <node lat='49.5135613' lon='6.0095049' ... > */
implicit val node: Parser[Any, Node] = (
  Parser.forMandatoryAttribute("lat").map(_.toDouble) ~
    Parser.forMandatoryAttribute("lon").map(_.toDouble) ~
    elementData
).as(Node)

// ... more parsers ...

/** The master parser.
  *
  * ===Usage===
  * {{{
  * val xml: InputStream = new FileInputStream("somefile.osm")
  *
  * val res: Try[(List[Node], List[Way], List[Relation])] = Element.elements.parse(xml)
  * }}}
  */
val elements: Parser[Any, (List[Node], List[Way], List[Relation])] = (
  Splitter("osm" \ "node").asListOf[Node] ~
    Splitter("osm" \ "way").asListOf[Way] ~
    Splitter("osm" \ "relation").asListOf[Relation]
).as({ case (ns, ws, rs) => (ns, ws, rs) })
```

Where the essential pattern is:

1. Tell `xml-spac` what fields you want
2. Give it a way to construct an object after parsing some fields (the `.as(Foo)`)

One could imagine the SPC variant to look like:

```scala
implicit val node: Parser[Node] = Node <~ attr "lat" ~ attr "lon" ~ elementData
```

or the `parsec`:

```haskell
node :: Parser Node
node = Node <$> attr "lat" <*> attr "lon" <*> elementData
```

Yes, the `xml-spac` version is a bit verbose, but it does the job.

Here's a full `xml-spac` example from [my code
playground](https://github.com/fosskers/playground/blob/master/scala/xml-spac/src/main/scala/playground/XML.scala).

So by Step 2, we've parsed all our XML into usable Scala objects. Now what?

Conversion to GeoTrellis `Feature`s
-----------------------------------

### Relations

Here be dragons, and Relations birthed them hence. Recall that:

1. Each Relation has a `type` tag, which can be set to anything
2. Relation store metadata relavent to the entire structure
3. Relations can refer to other Relations, forming Relation Graphs

(1) means that we (the GeoTrellis team) have to draw a line in the sand for
which Relations we need to give special treatmeat when converting from
`Element` to `Feature`. Geometric Relations like `multipolygon` and
`boundary` are obvious, but further out than that and the answer isn't so
clear.

One example of a "non-geometric Relation" would be a bus route, composed of
many Nodes and Ways. Each single Node could be a busstop or bus depot, while
the Ways show the actual route. In an actual display of a VectorTile
containing this bus route, it would be good to see each busstop on the map,
and also be able to confirm (via mouse-over metadata display, etc.) that any
given node is in fact a stop. The Nodes themselves (in the OSM DB or XML
form) have no idea they're part of a route. In fact, one stop could be a
member of multiple routes, and thus multiple Relations. *Relations know who
they contain, but not the other way around.* This is where (2) comes in.
Relations store the structure-wide metadata, but for the final Node to know
about it, the Relation's metadata has to be disseminated across its members.

And how hard could that be? Well, recall (3). What if your bus route is
itself pointed to by a higher Relation? Now we have Relation Graphs to deal
with. After consulting with OSM contributors in [their
IRC](http://irc.openstreetmap.org/), the concensus is that while these
Relations *should* form Tree structures, there's no guarantee that they will
because of the permissive way their XML works (i.e. Relations can refer to
anything). There's also no guarantee that the Graphs found won't be cyclic.

Our answer is to apply Graph theory and force a Tree structure, essentially
declaring Relation Graphs to be illegal. We ported an [established `Graph`
implementation](https://downloads.haskell.org/~ghc/latest/docs/html/libraries/containers-0.5.7.1/Data-Graph.html)
to Scala and gave it the operation:

```scala
topologicalForest[K, V]: Graph[K, V] => Forest[V]
```

where `type Forest[T] = Seq[Tree[T]]`.

This gives us a sensible method for interating across Relations and
disseminating their metadata to their children.

### Conversion Quirks

Polygons can be formed in three ways:

- From a single "closed" Way
- From a `multipolygon` Relation that designates which Ways to stitch together
- From a combination of the above two, forming holed Polygons

Here is a real example we can analyse. The gray-black areas/lines are our
converted data laid over the original OSM map.

![](multipoly.png)

Here, the bottom-most shape (containing the P) is a single, closed Way.
Above it is a two-Way multipolygon representing a forest with a large rock
to one side. The other black lines snaking about are single Ways. Here is
the same shot from Google Maps for context:

![](multipoly-gm.jpeg)

Quarry Rock itself is represented in OSM as a hole in the forest around it.
This is a pretty normal holed Polygon. We've seen the XML for this Relation
already, in the XML examples above. Interestingly, the bounding box which produced
all this was smaller than you might expect:

![](multipoly-bbox1.png)

This is due to how OpenStreetMap's "Export" utility works. The rough algorithm is:

1. Find all Nodes within the bounding box
2. Find all Ways which reference those Nodes
  - If not all Nodes referenced by a Way in this bbox are present, grab them
  anyway (non-recursively, this won't go out and find yet more Ways based on
  the new Nodes)
3. Find all Relations which reference those Nodes and Ways
  - Unlike Ways, even if a Relation refers to other Elements outside the
  bbox, *don't* fetch them. This prevents of flood of long Ways outside the
  bbox   from being added by mistake, producing large XML

So the full forest Polygon we see here is because of rule (2). What happens
if we grab the XML for a smaller bounding box?

![](multipoly-bbox2.png)

Hm, no Polygon present that represents the Rock. Checking the XML, the Way
for the Rock and the Relation for the whole forest *were* fetched, but
critically the outer Way for the forest edge was not. In this case, our
algorithm abandons the attempt to make the holed Polygon.

A similar thing can happen to stray Ways which ought to be stitched
together, but can't be due to the caveat in (3) above. This was a source of
strange Exceptions for a while. For now we abandon such Ways, knowing that
they *would* be properly fused if using the full data set.

Protobuf?
---------

The entire OSM Planet dataset as XML is around 50gb, while the same as
Protobuf data is around 30gb. The XML lends itself to being easily split
into  ten-of-thousands of parts as desired, but if a similar method can be
realized for Protobuf, then the overall parsing process to achieve
`RDD[Element]` should be much faster.

This approach would require more R&D up front, but may pay dividends.

Current Work
------------

I personally am moving into Step 1, so that code from Steps 2 and 3 can be
battle tested on some more realistically sized data sets. We're excited for
what this project will mean for both Azavea and the greater GIS community,
so stay tuned.
