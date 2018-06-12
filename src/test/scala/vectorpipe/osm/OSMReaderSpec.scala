package vectorpipe.osm


import vectorpipe.osm.internal._
import vectorpipe.osm.internal.functions._

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.jts._

import java.net.URI

import org.scalatest._


trait Session {
  implicit val ss =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("vectorpipe console")
      .config("spark.ui.enabled", true)
      .config("spark.dynamicAllocation.enabled", true)
      .config("spark.shuffle.compress", true)
      .config("spark.rdd.compress", true)
      .config("spark.shuffle.service.enabled", true)
      .config("spark.driver.maxResultSize", "1G")
      .config("spark.driver.memory", "3G")
      .config("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")
      .getOrCreate()
}


class OSMReaderSpec extends FunSpec with Session {
  describe("Reading a dataframe") {
    //val reader = OSMReader.apply(new URI("file:///tmp/india.orc"))(ss)

    /*
    it("should read") {
      try {
        val pointC = reader.pointFeaturesRDD.count
        val lineC = reader.lineFeaturesRDD.count
        val polyC = reader.polygonFeaturesRDD.count
        val mpolyC = reader.multiPolygonFeaturesRDD.count

        println("\n!!!!!!!!!!!!!!!!!!\n")
        println(s"This is the number of nodes: ${pointC}")
        println(s"This is the number of ways: ${lineC}")
        println(s"This is the number of polys: ${polyC}")
        println(s"This is the number of mpolys: ${mpolyC}")
        println("\n!!!!!!!!!!!!!!!!!!\n")
      } finally {
        ss.stop()
      }
    }
    */
  }
}
