---
layout: docs
title: "Reading OpenStreetMap Data"
section: "usage"
---

## From XML

OSM XML files usually appear with the extension `.osm`. Since the data is all string-based,
these files can be quite large compared to their PBF or ORC equivalents.

```tut:silent
import org.apache.spark._
import scala.util.{Success, Failure}
import vectorpipe._

implicit val sc: SparkContext = new SparkContext(
  new SparkConf().setMaster("local[*]").setAppName("xml-example")
)

val path: String = "/some/path/on/your/machine/foo.osm"

osm.fromLocalXML(path) match {
  case Failure(e) => { }  /* Parsing failed somehow... is the filepath correct? */
  case Success((ns,ws,rs)) => { }  /* (RDD[Node], RDD[Way], RDD[Relation]) */
}

sc.stop()
```

## From PBF

For the time being, `.osm.pbf` files can be used by first converting them to `.orc`
files using the [osm2orc](https://github.com/mojodna/osm2orc) tool, and then following
VectorPipe's ORC instructions given below.

## From ORC

You must first include an extra dependency to the `libraryDependencies` list in your `build.sbt`:

```
"org.apache.spark" %% "spark-hive" % "2.2.0"
```

And then we can read our OSM data in parallel via Spark. Notice the use of `SparkSession`
instead of `SparkContext` here:

```tut:silent
import org.apache.spark.sql._
import scala.util.{Success, Failure}
import vectorpipe._

implicit val ss: SparkSession =
  SparkSession.builder.master("local[*]").appName("orc-example").enableHiveSupport.getOrCreate

/* If you want to read an ORC file from S3, you must call this first */
// useS3(ss)

val path: String = "s3://bucket/key/foo.orc"
// val path: String = "/some/path/on/your/machine/foo.orc" /* If not using S3 */

osm.fromORC(path) match {
  case Failure(err) => { } /* Does the file exist? Do you have the right AWS credentials? */
  case Success((ns,ws,rs)) => { } /* (RDD[Node], RDD[Way], RDD[Relation]) */
}

ss.stop()
```

This approach will be particularly efficient when run on an EMR cluster, since
EMR clusters have privileged access to S3.
