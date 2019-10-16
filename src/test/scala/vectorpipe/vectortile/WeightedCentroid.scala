package vectorpipe.vectortile

import geotrellis.vector._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.jts.PointUDT
import org.apache.spark.sql.types._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}

class WeightedCentroid extends UserDefinedAggregateFunction {

  // Define the schema of the input data
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("point", PointUDT) :: StructField("weight", DoubleType) :: Nil)

  // Define the types of the intermediate data structure
  override def bufferSchema: StructType = StructType(
    StructField("x", DoubleType) :: StructField("y", DoubleType) :: StructField("weight", DoubleType) :: Nil
  )

  // Define the return type
  override def dataType: DataType = PointUDT

  // Does the function return the same value for the same input?
  override def deterministic: Boolean = true

  // Create a new, empty buffer structure
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0.0
    buffer(1) = 0.0
    buffer(2) = 0.0
  }

  // Combine a new input with an existing buffer
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val c = input.getAs[Point](0).getCoordinate
    val wt = input.getAs[Double](1)
    buffer(0) = buffer.getAs[Double](0) + c.x * wt
    buffer(1) = buffer.getAs[Double](1) + c.y * wt
    buffer(2) = buffer.getAs[Double](2) + wt
  }

  // Merge two intermediate buffers
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) + buffer2.getAs[Double](0)
    buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
    buffer1(2) = buffer1.getAs[Double](2) + buffer2.getAs[Double](2)
  }

  // Produce the final output from a Row encoded with the bufferSchema
  override def evaluate(buffer: Row): Any = {
    val wx = buffer.getDouble(0)
    val wy = buffer.getDouble(1)
    val wt = buffer.getDouble(2)
    (new GeometryFactory).createPoint(new Coordinate(wx/wt, wy/wt))
  }
}
