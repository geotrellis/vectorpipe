package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import vectorpipe.VectorPipe
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.ChangeStreamProcessor \
 *   target/scala-2.11/vectorpipe.jar \
 *   --augmented-diff-source s3://somewhere/diffs/
 */
object ChangeStreamProcessor
    extends CommandApp(
      name = "change-stream-processor",
      header = "Read OSM minutely diffs as a stream",
      main = {
        val changeSourceOpt = Opts
          .option[URI]("change-source",
                       short = "d",
                       metavar = "uri",
                       help = "Location of minutely diffs to process")
          .withDefault(new URI("https://planet.osm.org/replication/minute/"))
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
        val partitionCountOpt = Opts
          .option[Int]("partitions",
                       short = "p",
                       metavar = "partition count",
                       help = "Change partition count.")
          .orNone

        (changeSourceOpt, startSequenceOpt, endSequenceOpt, partitionCountOpt)
          .mapN {
            (changeSource, startSequence, endSequence, partitionCount) =>
              implicit val ss: SparkSession =
                VectorPipe.defaultSparkSessionWithJTS("ChangeStreamProcessor")

              val options = Map(Source.BaseURI -> changeSource.toString, Source.ProcessName -> "ChangeStreamProcessor") ++
                startSequence.map(s => Map(Source.StartSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                endSequence.map(s => Map(Source.EndSequence -> s.toString))
                  .getOrElse(Map.empty[String, String]) ++
                partitionCount.map(s => Map(Source.PartitionCount -> s.toString))
                  .getOrElse(Map.empty[String, String])

              val changes =
                ss.readStream.format(Source.Changes).options(options).load

              val query = changes.writeStream
                .format("console")
                .start

              query.awaitTermination()

              ss.stop()
          }
      }
    )
