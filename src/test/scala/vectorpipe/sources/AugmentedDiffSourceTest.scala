package vectorpipe.sources

import geotrellis.vector.Geometry
import org.apache.spark.internal.Logging
import org.scalatest.{FunSpec, Matchers}
import vectorpipe.TestEnvironment
import vectorpipe.model.ElementWithSequence
import vectorpipe.util.RobustFeature

class AugmentedDiffSourceSpec extends FunSpec with TestEnvironment with Matchers {

  import ss.implicits._

  describe("Timestamp to sequence conversion") {
    it("should provide a round trip for simple conversion") {
      AugmentedDiffSource.timestampToSequence(AugmentedDiffSource.sequenceToTimestamp(3700047)) should be (3700047)
    }

    it("should provide a round trip for column functions") {
      val df = ss.createDataset(Seq(3700047)).toDF
      (df.select(AugmentedDiffSource.sequenceToTimestamp('value) as 'time)
         .select(AugmentedDiffSource.timestampToSequence('time) as 'value)
         .first
         .getLong(0)) should be (3700047)
    }
  }

}

class LogErrors extends AugmentedDiffSourceErrorHandler with Logging {
  override def handle(sequence: Int, feature: RobustFeature[Geometry, ElementWithSequence]) = {
    logWarning(s"Error in sequence ${sequence} for feature with metadata: ${feature.data}")
  }
}
