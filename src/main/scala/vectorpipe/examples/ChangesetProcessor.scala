package vectorpipe.examples

import java.net.URI

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.sql._
import vectorpipe.VectorPipe
import vectorpipe.model.Changeset
import vectorpipe.sources.Source

/*
 * Usage example:
 *
 * sbt assembly
 *
 * spark-submit \
 *   --class vectorpipe.examples.ChangesetProcessor \
 *   target/scala-2.11/vectorpipe.jar
 */
object ChangesetProcessor
  extends CommandApp(
    name = "changeset-processor",
    header = "Read changesets between start sequence and end sequence",
    main = {
      val changesetSourceOpt =
        Opts.option[URI]("changeset-source",
          short = "c",
          metavar = "uri",
          help = "Location of changesets to process"
        ).withDefault(new URI("https://planet.osm.org/replication/changesets/"))
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

      (changesetSourceOpt, startSequenceOpt, endSequenceOpt)
        .mapN {
          (changesetSource, startSequence, endSequence) =>
            implicit val ss: SparkSession =
              VectorPipe.defaultSparkSessionWithJTS("ChangesetProcessor")

            import ss.implicits._

            val options = Map(Source.BaseURI -> changesetSource.toString) ++
              startSequence
                .map(s => Map(Source.StartSequence -> s.toString))
                .getOrElse(Map.empty[String, String]) ++
              endSequence
                .map(s => Map(Source.EndSequence -> s.toString))
                .getOrElse(Map.empty[String, String])

            val changes =
              ss.read.format(Source.Changesets).options(options).load

            // aggregations are triggered when an event with a later timestamp ("event time") is received
            // changes.select('sequence).distinct.show
            changes.as[Changeset].show

            ss.stop()
        }
    }
  )
