name := """vectorpipe"""

version := "1.0.0-SNAPSHOT"

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
  "org.apache.spark"            %% "spark-hive"            % "2.2.0" % "provided",
  "org.apache.hadoop"           %  "hadoop-aws"            % "2.8.1"
    exclude("javax.servlet",     "servlet-api")
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("org.mortbay.jetty", "servlet-api"),
  "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.2.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.2.0-SNAPSHOT",
  "org.scalatest"               %% "scalatest"             % "2.2.0" % "test",
  "org.typelevel"               %% "cats"                  % "0.9.0",
  "org.spire-math"              %% "spire"                 % "0.13.0"
)

parallelExecution in Test := false

/* Microsite Settings */

/* To generate the microsite locally, use `sbt makeMicrosite`.
 * To publish the site to Github, use `sbt publishMicrosite`.
 */

enablePlugins(MicrositesPlugin)
enablePlugins(SiteScaladocPlugin)

micrositeName := "VectorPipe"
micrositeDescription := "Convert Vector data into VectorTiles"
micrositeAuthor := "GeoTrellis Team at Azavea"
micrositeGitterChannel := false
micrositeOrganizationHomepage := "https://www.azavea.com/"
micrositeGithubOwner := "geotrellis"
micrositeGithubRepo := "vectorpipe"
micrositeBaseUrl := "/vectorpipe"
micrositeDocumentationUrl := "/vectorpipe/latest/api" /* Location of Scaladocs */
