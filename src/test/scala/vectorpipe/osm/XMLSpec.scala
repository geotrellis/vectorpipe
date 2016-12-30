package vectorpipe.osm

import java.io.InputStream
import org.scalatest._

import java.io.FileInputStream
import scala.util.Try

// --- //

class XMLSpec extends FunSpec with Matchers {

  describe("XML Parsing") {
    it("8-shaped Multipolygon") {
      val xml: InputStream = new FileInputStream("8shapedmultipolygon.osm")

      val res: Try[(List[Node], List[Way], List[Relation])] = Element.elements.parse(xml)

      assert(res.isSuccess)
    }
  }
}
