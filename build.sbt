name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("azavea", "maven")

libraryDependencies ++= Seq(
  "com.azavea"                  %% "scaliper"              % "0.5.0-SNAPSHOT" % "test",
  "io.dylemma"                  %% "xml-spac"              % "0.3-SNAPSHOT",  // Published locally
  "org.apache.spark"            %% "spark-core"            % "2.1.0",  // TODO add "provided" later
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.1.0-SNAPSHOT",
  "org.scalatest"               %% "scalatest"             % "2.2.0" % "test",
  "org.scalaz"                  %% "scalaz-core"           % "7.2.9",
  "org.spire-math"              %% "spire"                 % "0.13.0"
)

parallelExecution in Test := false
