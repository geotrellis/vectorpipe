name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "io.dylemma"                  %% "xml-spac"              % "0.3-SNAPSHOT",  // Published locally
  "org.apache.spark"            %% "spark-core"            % "2.1.0",  // TODO add "provided" later
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.0.0",
  "org.scalatest"               %% "scalatest"             % "3.0.0" % "test",
  "org.scalaz"                  %% "scalaz-core"           % "7.2.8",
  "org.spire-math"              %% "spire"                 % "0.13.0"
)
