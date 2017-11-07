name := """vectorpipe"""

version := "1.0.0-SNAPSHOT"

organization := "com.azavea"

organizationName := "Azavea"

scalaVersion in ThisBuild := "2.11.11"

val common = Seq(
  resolvers ++= Seq(
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    Resolver.bintrayRepo("azavea", "maven")
  ),

  scalacOptions := Seq(
    "-deprecation",
    "-Ypartial-unification"
  ),

  /* For Monocle's Lens auto-generation */
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full),

  libraryDependencies ++= Seq(
    "com.github.julien-truffaut"  %% "monocle-core"          % "1.5.0-cats-M2",
    "com.github.julien-truffaut"  %% "monocle-macro"         % "1.5.0-cats-M2",
    "io.dylemma"                  %% "xml-spac"              % "0.3",
    "org.apache.hadoop"           %  "hadoop-aws"            % "2.8.1" % "provided",
    "org.apache.spark"            %% "spark-hive"            % "2.2.0" % "provided",
    "org.locationtech.geotrellis" %% "geotrellis-spark"      % "1.2.0-SNAPSHOT",
    "org.locationtech.geotrellis" %% "geotrellis-util"       % "1.2.0-SNAPSHOT",
    "org.locationtech.geotrellis" %% "geotrellis-vector"     % "1.2.0-SNAPSHOT",
    "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "1.2.0-SNAPSHOT",
    "org.scalatest"               %% "scalatest"             % "3.0.1" % "test",
    "org.spire-math"              %% "spire"                 % "0.13.0",
    "org.typelevel"               %% "cats-core"             % "1.0.0-RC1"
  ),

  parallelExecution in Test := false
)

lazy val lib = project.in(file(".")).settings(common)

/* Benchmarking suite.
 * Benchmarks can be executed by first switching to the `bench` project and then by running:
      jmh:run -t 1 -f 1 -wi 5 -i 5 .*Bench.*
 */
lazy val bench = project.in(file("bench")).settings(common).dependsOn(lib).enablePlugins(JmhPlugin)

/* Microsite Settings
 *
 * To generate the microsite locally, use `sbt makeMicrosite`.
 * To publish the site to Github, use `sbt publishMicrosite`.
 *
 * Spark deps must not be marked `provided` while doing these, or you will get errors.
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
micrositeDocumentationUrl := "/vectorpipe/latest/api/#vectorpipe.VectorPipe$" /* Location of Scaladocs */
