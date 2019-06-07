package vectorpipe.vectortile

import vectorpipe.vectortile.export.SaveToS3

import com.amazonaws.services.s3.model.CannedAccessControlList._
import geotrellis.spark.SpatialKey
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.zcurve.Z2
import geotrellis.spark.io.s3._
import geotrellis.vectortile._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import java.net.URI
import java.io.ByteArrayOutputStream
import java.util.zip.{GZIPOutputStream, ZipEntry, ZipOutputStream}

package object export {
  def saveVectorTiles(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: URI): Unit = {
    uri.getScheme match {
      case "s3" =>
        val path = uri.getPath
        val prefix = path.stripPrefix("/").stripSuffix("/")
        saveToS3(vectorTiles, zoom, uri.getAuthority, prefix)
      case _ =>
        saveHadoop(vectorTiles, zoom, uri)
    }
  }

  def saveVectorTiles(vectorTiles: Dataset[(SpatialKey, Array[Byte])], zoom: Int, uri: URI): Unit = {
    uri.getScheme match {
      case "s3" =>
        val path = uri.getPath
        val prefix = path.stripPrefix("/").stripSuffix("/")
        saveToS3(vectorTiles, zoom, uri.getAuthority, prefix)
      case _ =>
        saveHadoop(vectorTiles, zoom, uri)
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

  private def saveToS3(vectorTiles: Dataset[(SpatialKey, Array[Byte])], zoom: Int, bucket: String, prefix: String) = {
    import vectorTiles.sparkSession.implicits._

    SaveToS3(
      vectorTiles
        .map { tup: (SpatialKey, Array[Byte]) =>
           val byteStream = new ByteArrayOutputStream()

           try {
             val gzipStream = new GZIPOutputStream(byteStream)
             try {
               gzipStream.write(tup._2)
             } finally {
               gzipStream.close()
             }
           } finally {
             byteStream.close()
           }

           (tup._1, byteStream.toByteArray)
         },
      { sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.mvt" },
      putObjectModifier = { o =>
        val md = o.getMetadata
        md.setContentEncoding("gzip")
        o.withMetadata(md).withCannedAcl(PublicRead)
      }
    )
  }

  private def saveHadoop(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, uri: URI) = {
    vectorTiles
      .mapValues(_.toBytes)
      .saveToHadoop({ sk: SpatialKey => s"${uri}/${zoom}/${sk.col}/${sk.row}.mvt" })
  }

  private def saveHadoop(vectorTiles: Dataset[(SpatialKey, Array[Byte])], zoom: Int, uri: URI) = {
    import vectorTiles.sparkSession.implicits._
    SaveToHadoop(
      vectorTiles.map { tup: (SpatialKey, Array[Byte]) => (tup._1, tup._2) },
      { sk: SpatialKey => s"${uri}/${zoom}/${sk.col}/${sk.row}.mvt" }
    )
  }

}
