name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.0.0",
  "org.spire-math"              %% "spire"                 % "0.11.0",
  "org.apache.spark"            %% "spark-core"            % "2.0.2" % "provided",
  "org.scalatest"               %% "scalatest"             % "3.0.0" % "test"
)
