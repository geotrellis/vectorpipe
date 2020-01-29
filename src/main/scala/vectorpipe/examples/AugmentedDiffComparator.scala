package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import vectorpipe.VectorPipe
import vectorpipe.model.AugmentedDiff
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.AugmentedDiffComparator \
 *   target/scala-2.11/vectorpipe.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object AugmentedDiffComparator
    extends CommandApp(
      name = "augmented-diff-comparator",
      header = "Zip two streams together and compare individual diffs",
      main = {
        val sourceOneOpt = Opts.argument[URI](metavar = "sourceUriOne")
        val sourceTwoOpt = Opts.argument[URI](metavar = "sourceUriTwo")
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, will start at sequence zero."
          )
          .orNone
        val endSequenceOpt = Opts
          .option[Int](
            "end-sequence",
            short = "e",
            metavar = "sequence",
            help = "Ending sequence. If absent, the current (remote) sequence will be used."
          )
          .orNone

        (sourceOneOpt, sourceTwoOpt, startSequenceOpt, endSequenceOpt)
          .mapN {
            (sourceOne, sourceTwo, startSequence, endSequence) =>
              implicit val ss: SparkSession =
                VectorPipe.defaultSparkSessionWithJTS("AugmentedDiffProcessor")

              import ss.implicits._

              val optionsOne = Map(Source.BaseURI -> sourceOne.toString) ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val diffsOne =
                ss.read.format(Source.AugmentedDiffs).options(optionsOne).load.as[AugmentedDiff]

              val optionsTwo = Map(Source.BaseURI -> sourceTwo.toString) ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val diffsTwo =
                ss.read.format(Source.AugmentedDiffs).options(optionsTwo).load.as[AugmentedDiff]

              val inconsistencies = diffsOne
                .as("d1")
                .withColumnRenamed("sequence", "d1_sequence")
                .withColumnRenamed("id", "d1_id")
                .join(
                  diffsTwo
                    .as("d2")
                    .withColumnRenamed("sequence", "d2_sequence")
                    .withColumnRenamed("id", "d2_id"),
                  'd1_sequence === 'd2_sequence, "full_outer"
                )
                .where(
                  ('d1_id.isNull && 'd2_id.isNotNull) || ('d1_id.isNotNull && 'd2_id.isNull)
                )
                .select('d1_id, 'd1_sequence, 'd2_id, 'd2_sequence)
              inconsistencies
                .write
                .csv("/Users/andrew/src/vectorpipe/data/aug-diff-compare")

              ss.stop()
          }
      }
    )
