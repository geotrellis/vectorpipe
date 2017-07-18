package vectorpipe

import java.nio.file.{Files, Paths}

import geotrellis.raster.TileLayout
import geotrellis.spark.SpatialKey
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import geotrellis.vectortile._
import org.scalatest._

// --- //

class AvroSpec extends FunSpec with Matchers {

  val layout = LayoutDefinition(
    Extent(0, 0, 4096, 4096),
    TileLayout(1, 1, 4096, 4096)
  )
  val tileExtent: Extent = layout.mapTransform(SpatialKey(0, 0))

  def read(file: String): Array[Byte] = {
    Files.readAllBytes(Paths.get(file))
  }

  describe("Avro Codec Isomorphism") {
    it("onepoint.mvt") {
      val bytes = read("data/onepoint.mvt")
      val tile = VectorTile.fromBytes(bytes, tileExtent)

      vectorTileCodec.decode(vectorTileCodec.encode(tile)) shouldBe tile
      tile.toBytes shouldBe bytes
    }

    it("linestring.mvt") {
      val bytes = read("data/linestring.mvt")
      val tile = VectorTile.fromBytes(bytes, tileExtent)

      vectorTileCodec.decode(vectorTileCodec.encode(tile)) shouldBe tile
      tile.toBytes shouldBe bytes
    }

    it("polygon.mvt") {
      val bytes = read("data/polygon.mvt")
      val tile = VectorTile.fromBytes(bytes, tileExtent)

      vectorTileCodec.decode(vectorTileCodec.encode(tile)) shouldBe tile
      tile.toBytes shouldBe bytes
    }

    /* This test does not check for byte-to-byte equivalence, as that
     * will never happen with our codec. Since Feature-level metadata
     * is stored as a Map, when reencoding these to Protobuf data,
     * the key/value pairs are rewritten in an arbitrary order.
     */
    it("roads.mvt") {
      val bytes = read("data/roads.mvt")
      val tile = VectorTile.fromBytes(bytes, tileExtent)

      val decoded = vectorTileCodec.decode(vectorTileCodec.encode(tile))

      decoded.layers.keys should equal(tile.layers.keys)
      decoded.layers.values.map(_.points) should equal(tile.layers.values.map(_.points))
      decoded.layers.values.map(_.multiPoints) should equal(tile.layers.values.map(_.multiPoints))
      decoded.layers.values.map(_.lines) should equal(tile.layers.values.map(_.lines))
      decoded.layers.values.map(_.multiLines) should equal(tile.layers.values.map(_.multiLines))
      decoded.layers.values.map(_.polygons) should equal(tile.layers.values.map(_.polygons))
//      decoded.layers.values.map(_.multiPolygons) should equal(tile.layers.values.map(_.multiPolygons))
    }
  }
}
