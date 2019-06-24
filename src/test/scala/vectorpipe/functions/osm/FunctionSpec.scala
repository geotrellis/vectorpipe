package vectorpipe.functions.osm

import org.apache.spark.sql.Row
import org.scalatest.{FunSpec, Matchers}
import vectorpipe.TestEnvironment

class FunctionSpec extends FunSpec with TestEnvironment with Matchers {

  import ss.implicits._

  describe("isArea") {
    it("marks 'area=*' appropriately") {
      Seq(
        Map("area" -> "yes") -> true,
        Map("area" -> "YES") -> true,
        Map("area" -> "YeS") -> true,
        Map("area" -> "1") -> true,
        Map("area" -> "true") -> true,
        Map("area" -> "True") -> true,
        Map("area" -> "no") -> false,
        Map("area" -> "no") -> false,
        Map("area" -> "0") -> false,
        Map("area" -> "something") -> false,
        Map("area" -> "yes;no") -> true,
        Map("area" -> "yes; no") -> true,
        Map("area" -> "yes ; no") -> true,
        Map("area" -> "yes ;no") -> true
      )
        .toDF("tags", "value")
        .where(isArea('tags) =!= 'value)
        .count should equal(0)
    }

    it("respects area-keys") {
      Seq(
        Map("office" -> "architect") -> true,
        Map("waterway" -> "riverbank") -> true,
        Map("waterway" -> "canal") -> false,
        Map("aeroway" -> "aerodrome;apron") -> true,
        Map("aeroway" -> "aerodrome ; runway") -> true,
        Map("aeroway" -> "taxiway;runway") -> false
      )
        .toDF("tags", "value")
        .where(isArea('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isMultiPolygon") {
    it("marks multipolygons and boundaries appropriately") {
      Seq(
        Map("type" -> "multipolygon") -> true,
        Map("type" -> "boundary") -> true,
        Map("type" -> "route") -> false,
        Map("type" -> "multipolygon;boundary") -> true,
        Map("type" -> "multipolygon ; boundary") -> true
      )
        .toDF("tags", "value")
        .where(isMultiPolygon('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isRoute") {
    it("marks routes appropriately") {
      Seq(
        Map("type" -> "multipolygon") -> false,
        Map("type" -> "boundary") -> false,
        Map("type" -> "route") -> true,
        Map("type" -> "route;boundary") -> true,
        Map("type" -> "route ; boundary") -> true
      )
        .toDF("tags", "value")
        .where(isRoute('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isBuilding") {
    it("marks buildings appropriately") {
      Seq(
        Map("building" -> "yes") -> true,
        Map("building" -> "no") -> false,
        Map("building" -> "false") -> false,
        Map("building" -> "farm") -> true,
        Map("building" -> "farm;apartments") -> true
      )
        .toDF("tags", "value")
        .where(isBuilding('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isPOI") {
    it("marks POIs appropriately") {
      Seq(
        Map("amenity" -> "cafe") -> true,
        Map("shop" -> "bakery") -> true,
        Map("craft" -> "bakery") -> true,
        Map("office" -> "architect") -> true,
        Map("leisure" -> "disc_golf_course") -> true,
        Map("aeroway" -> "aerodrome") -> true,
        Map("highway" -> "motorway") -> false,
        Map("shop" -> "bakery ; dairy") -> true
      )
        .toDF("tags", "value")
        .where(isPOI('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isRoad") {
    it("marks roads appropriately") {
      Seq(
        Map("highway" -> "motorway") -> true,
        Map("highway" -> "path") -> true,
        Map("highway" -> "path ;footway") -> true,
        Map("building" -> "yes") -> false
      )
        .toDF("tags", "value")
        .where(isRoad('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isCoastline") {
    it("marks coastline appropriately") {
      Seq(
        Map("natural" -> "coastline") -> true,
        Map("natural" -> "water") -> false,
        Map("natural" -> "coastline ; water") -> true
      )
        .toDF("tags", "value")
        .where(isCoastline('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isWaterway") {
    it("marks waterways appropriately") {
      Seq(
        Map("waterway" -> "river") -> true,
        Map("waterway" -> "riverbank") -> true,
        Map("waterway" -> "canal") -> true,
        Map("waterway" -> "stream") -> true,
        Map("waterway" -> "brook") -> true,
        Map("waterway" -> "drain") -> true,
        Map("waterway" -> "ditch") -> true,
        Map("waterway" -> "dam") -> true,
        Map("waterway" -> "weir") -> true,
        Map("waterway" -> "waterfall") -> true,
        Map("waterway" -> "pressurised") -> true,
        Map("waterway" -> "fuel") -> false,
        Map("waterway" -> "canal ; stream") -> true,
        Map("waterway" -> "canal ; fuel") -> true
      )
        .toDF("tags", "value")
        .where(isWaterway('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("removeUninterestingTags") {
    it("drops uninteresting tags") {
      Seq(
        Map("building" -> "yes", "created_by" -> "JOSM")
      )
        .toDF("tags")
        .withColumn("tags", removeUninterestingTags('tags))
        .collect() should equal(Array(Row(Map("building" -> "yes"))))
    }

    it("drops uninteresting single tags") {
      Seq(
        Map("building" -> "yes", "colour" -> "grey"),
        Map("colour" -> "grey")
      )
        .toDF("tags")
        .withColumn("tags", removeUninterestingTags('tags))
        .collect() should equal(Array(Row(Map("building" -> "yes", "colour" -> "grey")), Row(Map.empty)))
    }

    it("drops uninteresting prefixed tags") {
      Seq(
        Map("highway" -> "motorway", "tiger:reviewed" -> "no"),
        Map("building" -> "yes", "CLC:something" -> "something")
      )
        .toDF("tags")
        .withColumn("tags", removeUninterestingTags('tags))
        .collect() should equal(Array(Row(Map("highway" -> "motorway")), Row(Map("building" -> "yes"))))
    }

    it("drops tags with invalid keys") {
      Seq(
        Map("highway" -> "motorway", "k=v" -> "value"),
        Map("building" -> "yes", "land use" -> "something")
      )
        .toDF("tags")
        .withColumn("tags", removeUninterestingTags('tags))
        .collect() should equal(Array(Row(Map("highway" -> "motorway")), Row(Map("building" -> "yes"))))
    }
  }
}
