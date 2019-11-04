package vectorpipe.sources

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import vectorpipe.model.Changeset

import scala.collection.JavaConverters._

case class ChangesetStreamBatchTask(baseURI: URI, sequences: Seq[Int])
    extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new ChangesetStreamBatchReader(baseURI, sequences)
}

class ChangesetStreamBatchReader(baseURI: URI, sequences: Seq[Int])
    extends ReplicationStreamBatchReader[Changeset](baseURI, sequences) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Changeset] =
    ChangesetSource.getChangeset(baseURI, sequence)
}

class ChangesetMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader[Changeset](options, checkpointLocation) {
  private val baseURI = new URI(
    options
      .get(Source.BaseURI)
      .orElse("https://planet.osm.org/replication/changesets/")
  )

  override def getCurrentSequence: Option[Int] =
    ChangesetSource.getCurrentSequence(baseURI).map(_.sequence.toInt)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    sequenceRange
      .map(
        seq => ChangesetStreamBatchTask(baseURI, Seq(seq)).asInstanceOf[InputPartition[InternalRow]]
      )
      .asJava
}
