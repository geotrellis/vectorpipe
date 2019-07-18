package vectorpipe

import org.scalatest._
import vectorpipe.{internal => ProcessOSM}
import vectorpipe.functions.osm.ensureCompressedMembers

class ProcessOSMTest extends FunSpec with TestEnvironment with Matchers {
  val orcFile = getClass.getResource("/isle-of-man-latest.osm.orc").getPath

  val elements = ss.read.orc(orcFile)
  val nodes = ProcessOSM.preprocessNodes(elements).cache
  val nodeGeoms = ProcessOSM.constructPointGeometries(nodes).cache
  val wayGeoms = ProcessOSM.reconstructWayGeometries(elements, nodes).cache
  val relationGeoms = ProcessOSM.reconstructRelationGeometries(ensureCompressedMembers(elements), wayGeoms).cache

  it("parses isle of man nodes") {
    info(s"Nodes: ${nodeGeoms.count}")
  }

  it("parses isle of man ways") {
    info(s"Ways: ${wayGeoms.count}")
  }

  it("parses isle of man relations") {
    info(s"Relations: ${relationGeoms.count}")
  }
}
