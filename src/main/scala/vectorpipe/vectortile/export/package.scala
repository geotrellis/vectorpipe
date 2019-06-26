package vectorpipe.vectortile

import com.amazonaws.services.s3.model.CannedAccessControlList._
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.io.s3._
import geotrellis.vectortile._
import org.apache.spark.rdd.RDD
import java.net.URI
import java.io.ByteArrayOutputStream
import java.util.zip.{GZIPOutputStream, ZipEntry, ZipOutputStream}

import org.apache.spark.sql.Dataset
import vectorpipe.Tile

package object export {
  def saveVectorTiles(vectorTiles: Dataset[Tile], zoom: Int, uri: URI): Unit = {
    import vectorTiles.sparkSession.implicits._

    val vts = vectorTiles.map(x => (x.sk, x.tile)).rdd

    uri.getScheme match {
      case "s3" =>
        val path = uri.getPath
        val prefix = path.stripPrefix("/").stripSuffix("/")
        saveToS3(vts, zoom, uri.getAuthority, prefix)
      case _ =>
        saveHadoop(vts, zoom, uri)
    }
  }

  private def saveToS3(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    vectorTiles
      .mapValues { tile =>
        val byteStream = new ByteArrayOutputStream()

        try {
          val gzipStream = new GZIPOutputStream(byteStream)
          try {
            gzipStream.write(tile.toBytes)
          } finally {
            gzipStream.close()
          }
        } finally {
          byteStream.close()
        }

        byteStream.toByteArray
      }
      .saveToS3(
        { sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.mvt" },
        putObjectModifier = { o =>
          val md = o.getMetadata

          md.setContentEncoding("gzip")

          o
            .withMetadata(md)
            .withCannedAcl(PublicRead)
        })
  }

  private def saveHadoop(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: URI) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToHadoop({ sk: SpatialKey => s"${uri}/${zoom}/${sk.col}/${sk.row}.mvt" })
  }

}
