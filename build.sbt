name := """vectorpipe"""

version := "1.0.0"

organization := "com.azavea"
organizationName := "Azavea"

scalaVersion := "2.11.11"

resolvers += Resolver.bintrayRepo("azavea", "maven")

scalacOptions := Seq(
  "-Ypartial-unification"
)

libraryDependencies ++= Seq(
  "com.azavea"                  %% "scaliper"              % "0.5.0-SNAPSHOT" % "test",
  "io.dylemma"                  %% "xml-spac"              % "0.3",
  "org.apache.spark"            %% "spark-core"            % "2.2.0",  // TODO add "provided" later
  "org.apache.spark"            %% "spark-sql"             % "2.2.0",
  "org.apache.hadoop"           %  "hadoop-aws"            % "2.8.1",
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.1.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.1.0-SNAPSHOT",
  "org.scalatest"               %% "scalatest"             % "2.2.0" % "test",
  "org.typelevel"               %% "cats"                  % "0.9.0",
  "org.spire-math"              %% "spire"                 % "0.13.0"
)

parallelExecution in Test := false

/* Microsite Settings */

enablePlugins(MicrositesPlugin)

micrositeName := "VectorPipe"
micrositeDescription := "Convert Vector data into VectorTiles"
micrositeAuthor := "GeoTrellis Team at Azavea"
micrositeGitterChannel := false
micrositeOrganizationHomepage := "https://www.azavea.com/"
micrositeGithubOwner := "geotrellis"
micrositeGithubRepo := "vectorpipe"
micrositeBaseUrl := "/vectorpipe"
