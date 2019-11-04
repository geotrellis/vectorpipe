package vectorpipe.sources

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import vectorpipe.model.Changeset

import scala.collection.JavaConverters._
import scala.util.Random

case class ChangesetReader(options: DataSourceOptions)
    extends ReplicationReader[Changeset](options) {
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    // prevent sequential diffs from being assigned to the same task
    val sequences = Random.shuffle((startSequence to endSequence).toList)

    sequences
      .grouped(Math.max(1, sequences.length / partitionCount))
      .toList
      .map(
        ChangesetStreamBatchTask(baseURI, _)
          .asInstanceOf[InputPartition[InternalRow]]
      )
      .asJava
  }

  override protected def getCurrentSequence: Option[Int] =
    ChangesetSource.getCurrentSequence(baseURI).map(_.sequence.toInt)

  private def baseURI =
    new URI(
      options
        .get(Source.BaseURI)
        .orElse("https://planet.osm.org/replication/changesets/"))
}
