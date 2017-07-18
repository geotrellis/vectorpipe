import java.nio.ByteBuffer

import com.amazonaws.auth.{ AWSCredentialsProvider, DefaultAWSCredentialsProviderChain }
import geotrellis.spark.io.avro._
import geotrellis.spark.io.avro.codecs.Implicits._
import geotrellis.vector.Extent
import geotrellis.vectortile.VectorTile
import org.apache.avro._
import org.apache.avro.generic._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql._

// --- //

/** VectorPipe is a library for mass conversion of OSM data to VectorTiles,
  * using GeoTrellis and Apache Spark - See the [[VectorPipe]] object for usage
  * instructions.
  */
package object vectorpipe {

  /** Configure Spark to read files from S3 - ''Mutates your underlying Hadoop Configuration!'' */
  def useS3(ss: SparkSession): Unit = {
    val creds: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain
    val config: Configuration = ss.sparkContext.hadoopConfiguration
    config.set("fs.s3.impl", classOf[org.apache.hadoop.fs.s3native.NativeS3FileSystem].getName)
    config.set("fs.s3.awsAccessKeyId", creds.getCredentials.getAWSAccessKeyId)
    config.set("fs.s3.awsSecretAccessKey", creds.getCredentials.getAWSSecretKey)
  }

  /** Encode a [[VectorTile]] via Avro. This is the glue for Layer IO. */
  implicit val vectorTileCodec = new AvroRecordCodec[VectorTile] {
    def schema: Schema = SchemaBuilder
      .record("VectorTile").namespace("geotrellis.vectortile")
      .fields()
      .name("bytes").`type`().bytesType().noDefault()
      .name("extent").`type`(extentCodec.schema).noDefault()
      .endRecord()

    def encode(tile: VectorTile, rec: GenericRecord): Unit = {
      rec.put("bytes", ByteBuffer.wrap(tile.toBytes))
      rec.put("extent", extentCodec.encode(tile.tileExtent))
    }

    def decode(rec: GenericRecord): VectorTile = {
      val bytes: Array[Byte] = rec[ByteBuffer]("bytes").array
      val extent: Extent = extentCodec.decode(rec[GenericRecord]("extent"))

      VectorTile.fromBytes(bytes, extent)
    }
  }
}
