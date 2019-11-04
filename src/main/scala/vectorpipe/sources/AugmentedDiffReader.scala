package vectorpipe.sources

import java.net.URI
import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.InputPartition
import vectorpipe.model.AugmentedDiff

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._
import scala.util.Random

case class AugmentedDiffReader(options: DataSourceOptions)
    extends ReplicationReader[AugmentedDiff](options) {
  override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
    // prevent sequential diffs from being assigned to the same task
    val sequences = Random.shuffle((startSequence to endSequence).toList)

    sequences
      .grouped(Math.max(1, sequences.length / partitionCount))
      .toList
      .map(
        AugmentedDiffStreamBatchTask(baseURI, _)
          .asInstanceOf[InputPartition[InternalRow]]
      )
      .asJava
  }

  private def baseURI: URI =
    options
      .get(Source.BaseURI)
      .asScala
      .map(new URI(_))
      .getOrElse(
        throw new RuntimeException(
          s"${Source.BaseURI} is a required option for ${Source.AugmentedDiffs}"
        )
      )

  override def getCurrentSequence: Option[Int] = AugmentedDiffSource.getCurrentSequence(baseURI)
}
