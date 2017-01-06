name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"

scalaVersion := "2.11.8"

/* For `@Lenses` annotations */
addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full)

libraryDependencies ++= Seq(
//  "com.github.julien-truffaut"  %% "monocle-core"          % "1.4.0-M1",
//  "com.github.julien-truffaut"  %% "monocle-law"           % "1.4.0-M1" % "test",
//  "com.github.julien-truffaut"  %% "monocle-macro"         % "1.4.0-M1",
  "io.dylemma"                  %% "xml-spac"              % "0.3-SNAPSHOT",  // Published locally
  "org.apache.spark"            %% "spark-core"            % "2.1.0",  // TODO add "provided" later
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.0.0",
  "org.scalatest"               %% "scalatest"             % "3.0.0" % "test",
  "org.scalaz"                  %% "scalaz-core"           % "7.2.8",
  "org.spire-math"              %% "spire"                 % "0.11.0"
)
