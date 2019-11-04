package vectorpipe.sources

import java.net.URI

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader

import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import scala.reflect.runtime.universe.TypeTag

abstract class ReplicationStreamBatchReader[T <: Product: TypeTag](baseURI: URI,
                                                                   sequences: Seq[Int])
    extends InputPartitionReader[InternalRow]
    with Logging {
  org.apache.spark.sql.jts.registerTypes()
  private lazy val rowEncoder = RowEncoder(encoder.schema).resolveAndBind()
  protected var index: Int = -1
  protected var items: Vector[T] = _
  val Concurrency: Int = 8
  private lazy val encoder = ExpressionEncoder[T]

  override def next(): Boolean = {
    index += 1

    if (Option(items).isEmpty) {
      val parSequences = sequences.par
      val taskSupport = new ForkJoinTaskSupport(new ForkJoinPool(Concurrency))
      parSequences.tasksupport = taskSupport

      items = parSequences.flatMap(seq => getSequence(baseURI, seq)).toVector

      taskSupport.environment.shutdown()
    }

    index < items.length
  }

  override def get(): InternalRow = encoder.toRow(items(index))

  override def close(): Unit = Unit

  protected def getSequence(baseURI: URI, sequence: Int): Seq[T]
}
