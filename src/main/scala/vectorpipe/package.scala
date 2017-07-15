import com.amazonaws.auth.{ AWSCredentialsProvider, DefaultAWSCredentialsProviderChain }
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext

/** VectorPipe is a library for mass conversion of OSM data to VectorTiles,
  * using GeoTrellis and Apache Spark - See the [[VectorPipe]] object for usage
  * instructions.
  */
package object vectorpipe {
  /** Configure Spark to read files from S3 - ''Mutates your Hadoop Configuration!'' */
  def useS3(sc: SparkContext): Unit = {
    val creds: AWSCredentialsProvider = new DefaultAWSCredentialsProviderChain
    val config: Configuration = sc.hadoopConfiguration
    config.set("fs.s3.impl", classOf[org.apache.hadoop.fs.s3native.NativeS3FileSystem].getName)
    config.set("fs.s3.awsAccessKeyId", creds.getCredentials.getAWSAccessKeyId)
    config.set("fs.s3.awsSecretAccessKey", creds.getCredentials.getAWSSecretKey)
  }
}
