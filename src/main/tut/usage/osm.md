---
layout: docs
title: "Reading OpenStreetMap Data"
section: "usage"
---

## From XML

```tut:silent
import org.apache.spark._
import org.apache.spark.rdd.RDD
import vectorpipe._

implicit val sc: SparkContext = new SparkContext(
  new SparkConf().setMaster("local[*]").setAppName("xml-example")
)

val path: String = ""

osm.fromLocalXML(path) match {
  case Left(e) => { }  /* Parsing failed somehow... is the filepath correct? */
  case Right((ns,ws,rs)) => { }  /* (RDD[Node], RDD[Way], RDD[Relation]) */
}

sc.stop()
```

## From PBF

Forthcoming.

## From ORC

Forthcoming.
