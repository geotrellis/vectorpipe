package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import vectorpipe.VectorPipe
import vectorpipe.model.Change
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.ChangeProcessor \
 *   target/scala-2.11/vectorpipe.jar
 */
object ChangeProcessor
  extends CommandApp(
    name = "change-processor",
    header = "Read minutely changes from start sequence to end sequence",
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

      (changeSourceOpt, startSequenceOpt, endSequenceOpt)
        .mapN {
          (changeSource, startSequence, endSequence) =>
            implicit val ss: SparkSession =
              VectorPipe.defaultSparkSessionWithJTS("ChangeProcessor")

            import ss.implicits._

            val options = Map(Source.BaseURI -> changeSource.toString) ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes =
              ss.read.format(Source.Changes).options(options).load

            // aggregations are triggered when an event with a later timestamp ("event time") is received
            // changes.select('sequence).distinct.show
            changes.as[Change].show

            ss.stop()
        }
    }
  )
