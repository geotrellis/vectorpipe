package vectorpipe.sources

import java.net.URI
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import vectorpipe.model.AugmentedDiff

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

case class AugmentedDiffStreamBatchTask(baseURI: URI, sequences: Seq[Int], handler: (Int, AugmentedDiffSource.RF) => Unit)
    extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] =
    AugmentedDiffStreamBatchReader(baseURI, sequences, handler)
}

case class AugmentedDiffStreamBatchReader(baseURI: URI, sequences: Seq[Int], handler: (Int, AugmentedDiffSource.RF) => Unit)
    extends ReplicationStreamBatchReader[AugmentedDiff](baseURI, sequences) {

  override def getSequence(baseURI: URI, sequence: Int): Seq[AugmentedDiff] =
    AugmentedDiffSource.getSequence(baseURI, sequence, handler)
}

case class AugmentedDiffMicroBatchReader(options: DataSourceOptions, checkpointLocation: String)
    extends ReplicationStreamMicroBatchReader[AugmentedDiff](options, checkpointLocation)
    with Logging {

  override def getCurrentSequence: Option[Int] =
    AugmentedDiffSource.getCurrentSequence(baseURI)

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

  private def errorHandler: AugmentedDiffSourceErrorHandler = {
    val handlerClass = options
      .get(Source.ErrorHandler)
      .asScala
      .getOrElse("vectorpipe.sources.AugmentedDiffSourceErrorHandler")

    val handler = Class.forName(handlerClass).newInstance.asInstanceOf[AugmentedDiffSourceErrorHandler]
    handler.setOptions(options.asMap.asScala.toMap)
    handler
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] =
    sequenceRange
      .map(seq =>
        AugmentedDiffStreamBatchTask(baseURI, Seq(seq), errorHandler.handle).asInstanceOf[DataReaderFactory[Row]])
      .asJava
}
