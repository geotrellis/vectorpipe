package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import vectorpipe.VectorPipe
import vectorpipe.model.AugmentedDiff
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.AugmentedDiffProcessor \
 *   target/scala-2.11/vectorpipe.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object AugmentedDiffProcessor
    extends CommandApp(
      name = "augmented-diff-processor",
      header = "Read from augmented diffs",
      main = {
        val augmentedDiffSourceOpt = Opts.option[URI](
          "augmented-diff-source",
          short = "a",
          metavar = "uri",
          help = "Location of augmented diffs to process"
        )
        val startSequenceOpt = Opts
          .option[Int](
            "start-sequence",
            short = "s",
            metavar = "sequence",
            help = "Starting sequence. If absent, the current (remote) sequence will be used."
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

        (augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt)
          .mapN {
            (augmentedDiffSource, startSequence, endSequence) =>
              implicit val ss: SparkSession =
                VectorPipe.defaultSparkSessionWithJTS("AugmentedDiffProcessor")

              import ss.implicits._

              val options = Map(Source.BaseURI -> augmentedDiffSource.toString) ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val geoms =
                ss.read.format(Source.AugmentedDiffs).options(options).load

              // aggregations are triggered when an event with a later timestamp ("event time") is received
              // geoms.select('sequence).distinct.show
              geoms.as[AugmentedDiff].show

              ss.stop()
          }
      }
    )
