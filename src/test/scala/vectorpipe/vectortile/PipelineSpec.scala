package vectorpipe.vectortile

import java.net.URI

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{isnull, lit}
import org.locationtech.geomesa.spark.jts._
import org.scalatest._
import vectorpipe.{TestEnvironment, internal => vp, _}

class PipelineSpec extends FunSpec with TestEnvironment with Matchers {
  import ss.implicits._

  ss.withJTS
  val orcFile = getClass.getResource("/isle-of-man-latest.osm.orc").getPath
  val df = ss.read.orc(orcFile)

  describe("Vectortile Pipelines") {
    val nodes = vp.preprocessNodes(df, None)

    val nodeGeoms = nodes
      .filter(functions.not(isnull('lat)))
      .withColumn("geometry", st_makePoint('lon, 'lat))
      .drop("lat", "lon")
      .withColumn("weight", lit(1))
      .cache

    val wayGeoms = vp.reconstructWayGeometries(df, nodes).cache

    it("should generate a single zoom level") {
      val pipeline = TestPipeline("geometry", Some(new URI("file:///tmp/iom-tiles")), 16)
      VectorPipe(nodeGeoms, pipeline, VectorPipe.Options.forZoom(8))
    }

    it("should generate multiple zoom levels") {
      val pipeline = TestPipeline("geometry", Some(new URI("file:///tmp/iom-tiles-pyramid")), 16)
      VectorPipe(nodeGeoms, pipeline, VectorPipe.Options.forZoomRange(6, 8))
    }

    it("should generate multiple layers") {
      val pipeline = LayerTestPipeline("geom", Some(new URI("file:///tmp/iom-layers")))
      VectorPipe(wayGeoms, pipeline, VectorPipe.Options.forZoom(14))
    }
  }

}
