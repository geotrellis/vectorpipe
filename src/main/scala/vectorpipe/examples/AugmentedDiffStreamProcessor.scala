package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.vector.{Feature, Geometry}
import org.apache.spark.sql._
import vectorpipe.VectorPipe
import vectorpipe.model.ElementWithSequence
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.AugmentedDiffStreamProcessor \
 *   target/scala-2.11/vectorpipe.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object AugmentedDiffStreamProcessor
    extends CommandApp(
      name = "augmented-diff-stream-processor",
      header = "Read OSM augmented diffs as an open stream",
      main = {
        type AugmentedDiffFeature = Feature[Geometry, ElementWithSequence]

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
            help = "Ending sequence. If absent, this will be an infinite stream."
          )
          .orNone

        (augmentedDiffSourceOpt, startSequenceOpt, endSequenceOpt)
          .mapN {
            (augmentedDiffSource, startSequence, endSequence) =>
              implicit val ss: SparkSession =
                VectorPipe.defaultSparkSessionWithJTS("AugmentedDiffStreamProcessor")

              val options = Map(Source.BaseURI -> augmentedDiffSource.toString,
                                Source.ProcessName -> "AugmentedDiffStreamProcessor") ++
                startSequence
                  .map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence
                  .map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val geoms =
                ss.readStream.format(Source.AugmentedDiffs).options(options).load

              // aggregations are triggered when an event with a later timestamp ("event time") is received
              val query = geoms.writeStream
                .format("console")
                .start

              query.awaitTermination()

              ss.stop()
          }
      }
    )
