package vectorpipe.functions.osm

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
        Map("area" -> "something") -> false
      )
        .toDF("tags", "value")
        .where(isArea('tags) =!= 'value)
        .count should equal(0)
    }

    it("respects area-keys") {
      Seq(
        Map("office" -> "architect") -> true,
        Map("waterway" -> "riverbank") -> true,
        Map("waterway" -> "canal") -> false
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
        Map("type" -> "route") -> false
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
        Map("type" -> "route") -> true
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
        Map("building" -> "farm") -> true
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
        Map("highway" -> "motorway") -> false
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
        Map("building" -> "yes") -> false
      )
        .toDF("tags", "value")
        .where(isRoad('tags) =!= 'value)
        .count should equal(0)
    }
  }

  describe("isCoastline") {
    it("marks roads appropriately") {
      Seq(
        Map("natural" -> "coastline") -> true,
        Map("natural" -> "water") -> false
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
        Map("waterway" -> "fuel") -> false
      )
        .toDF("tags", "value")
        .where(isWaterway('tags) =!= 'value)
        .count should equal(0)
    }
  }
}
