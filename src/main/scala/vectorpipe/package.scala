import com.amazonaws.auth.{ AWSCredentialsProvider, DefaultAWSCredentialsProviderChain }
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
}
