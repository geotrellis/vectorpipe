name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.azavea.geotrellis" %% "geotrellis-util"       % "1.0.0-SNAPSHOT",
  "com.azavea.geotrellis" %% "geotrellis-vector"     % "1.0.0-SNAPSHOT",
  "com.azavea.geotrellis" %% "geotrellis-vectortile" % "1.0.0-SNAPSHOT",
  "org.apache.spark"      %% "spark-core"            % "2.0.1" % "provided",
  "org.scalatest"         %% "scalatest"             % "3.0.0" % "test"
)
