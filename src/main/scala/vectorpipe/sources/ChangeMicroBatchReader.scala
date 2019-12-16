package vectorpipe.sources

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}
import vectorpipe.model.Change

import scala.collection.JavaConverters._

case class ChangeStreamBatchTask(baseURI: URI, sequences: Seq[Int]) extends InputPartition[InternalRow] {
  override def createPartitionReader(): InputPartitionReader[InternalRow] =
    new ChangeStreamBatchReader(baseURI, sequences)
}

class ChangeStreamBatchReader(baseURI: URI, sequences: Seq[Int])
    extends ReplicationStreamBatchReader[Change](baseURI, sequences) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[Change] =
    ChangeSource.getSequence(baseURI, sequence)
}

case class ChangeMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader[Change](options, checkpointLocation) {
  private lazy val baseURI = new URI(
    options
      .get(Source.BaseURI)
      .orElse("https://planet.osm.org/replication/minute/")
  )

  override def getCurrentSequence: Option[Int] =
    ChangeSource.getCurrentSequence(baseURI)

  override def planInputPartitions(): util.List[InputPartition[InternalRow]] =
    sequenceRange
      .map(
        seq => ChangeStreamBatchTask(baseURI, Seq(seq)).asInstanceOf[InputPartition[InternalRow]]
      )
      .asJava
}
