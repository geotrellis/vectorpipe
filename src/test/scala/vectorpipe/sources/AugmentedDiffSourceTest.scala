package vectorpipe.sources

import org.scalatest.{FunSpec, Matchers}
import vectorpipe.TestEnvironment

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
