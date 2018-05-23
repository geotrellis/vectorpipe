import sbt._


object Dependencies {
  val gtSpark      = "org.locationtech.geotrellis" %% "geotrellis-spark"      % Version.geotrellis
  val gtVectorTile = "org.locationtech.geotrellis" %% "geotrellis-vectortile" % Version.geotrellis
  val gtShapefile  = "org.locationtech.geotrellis" %% "geotrellis-shapefile"  % Version.geotrellis
  val gtS3         = "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis
  val hbaseCommon  = "org.apache.hbase"             % "hbase-common"          % Version.hbase
  val hbaseClient  = "org.apache.hbase"             % "hbase-client"          % Version.hbase
  val hbaseServer  = "org.apache.hbase"             % "hbase-server"          % Version.hbase
  val sparkHive    = "org.apache.spark"            %% "spark-hive"            % Version.spark
  val spire        = "org.spire-math"              %% "spire"                 % Version.spire
  val cats         = "org.typelevel"               %% "cats-core"             % Version.cats
  val decline      = "com.monovore"                %% "decline"               % Version.decline
  val scalaTest    = "org.scalatest"               %% "scalatest"             % Version.scalaTest
}
