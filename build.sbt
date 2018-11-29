name := "vectorpipe"

description := "Convert Vector data to VectorTiles with GeoTrellis."

organization := "com.azavea"

organizationName := "Azavea"

val common = Seq(
  version := Version.vectorpipe,
  scalaVersion := Version.scala,
  crossScalaVersions := Version.crossScala,
  resolvers ++= Seq(
    "locationtech-releases" at "https://repo.locationtech.org/content/groups/releases",
    Resolver.bintrayRepo("azavea", "maven")
  ),

  initialCommands in console :=
    """
    import org.apache.spark._
    import org.apache.spark.sql._

    import org.locationtech.geomesa.spark.jts._

    import vectorpipe.osm._
    import vectorpipe.osm.internal._

    import java.net.URI

    implicit val ss =
      SparkSession
        .builder()
        .master("local[*]")
        .appName("vectorpipe console")
        .config("spark.ui.enabled", true)
        .getOrCreate()
        .withJTS
    """.stripMargin,

  scalacOptions := Seq(
    "-deprecation",
    "-Ypartial-unification",
    "-Ywarn-value-discard",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-language:implicitConversions",
    "-language:reflectiveCalls"
  ),

  resolvers ++= Seq(
    Resolver.bintrayRepo("lonelyplanet", "maven"),
    Resolver.bintrayRepo("bkirwi", "maven"), // Required for `decline` dependency
    "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
    "geosolutions" at "http://maven.geo-solutions.it/",
    "boundless" at "https://repo.boundlessgeo.com/main/",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/",
    "apache.commons.io" at "https://mvnrepository.com/artifact/commons-io/commons-io"
  ),

  scalacOptions in (Compile, doc) += "-groups",

  /* For Monocle's Lens auto-generation */
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full),

  libraryDependencies ++= Seq(
    "org.locationtech.geotrellis" %% "geotrellis-vectortile" % Version.geotrellis exclude("com.google.protobuf", "protobuf-java"),
    "org.locationtech.geotrellis" %% "geotrellis-s3"         % Version.geotrellis exclude("com.google.protobuf", "protobuf-java"),
    "org.locationtech.geomesa"    %% "geomesa-spark-jts"     % Version.geomesa,
    "org.apache.spark"            %% "spark-hive"            % Version.spark % "provided",
    "org.spire-math"              %% "spire"                 % Version.spire,
    "org.typelevel"               %% "cats-core"             % Version.cats,
    "com.monovore"                %% "decline"               % Version.decline,
    "org.scalatest"               %% "scalatest"             % Version.scalaTest % "test",
    "com.github.julien-truffaut"  %% "monocle-core"          % Version.monocle,
    "com.github.julien-truffaut"  %% "monocle-macro"         % Version.monocle,
    "com.github.seratch"          %% "awscala"               % "0.6.1",
    "org.apache.commons"           % "commons-compress"      % "1.16.1",
    "com.google.protobuf"          % "protobuf-java"         % "2.5.0",
    "io.dylemma"                  %% "xml-spac"              % "0.3",
    "javax.media"                  % "jai_core"              % "1.1.3" % "test" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar",
  ),

  parallelExecution in Test := false,
  fork in Test := false,
  testOptions in Test += Tests.Argument("-oDF")
)

val release = Seq(
  bintrayOrganization := Some("azavea"),
  bintrayRepository := "maven",
  bintrayVcsUrl := Some("https://github.com/geotrellis/vectorpipe.git"),
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  licenses += ("Apache-2.0", url("http://apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://geotrellis.github.io/vectorpipe/"))
)

lazy val lib = project.in(file(".")).settings(common, release)

assemblyShadeRules in assembly := {
  val shadePackage = "com.azavea.shaded.demo"
  Seq(
    ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-cassandra" % Version.geotrellis).inAll,
    ShadeRule.rename("io.netty.**" -> s"$shadePackage.io.netty.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-hbase" % Version.geotrellis).inAll,
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll
  )
}

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSEF.RSA" | "META-INF/ECLIPSEF.SF" => MergeStrategy.discard
  case "META-INF/ECLIPSE_.RSA" | "META-INF/ECLIPSE_.SF" => MergeStrategy.discard
  case s if s.startsWith("META-INF/") && s.endsWith("SF") => MergeStrategy.discard
  case s if s.startsWith("META-INF/") && s.endsWith("RSA") => MergeStrategy.discard
  case s if s.startsWith("META-INF/") && s.endsWith("DSA") => MergeStrategy.discard
  case _ => MergeStrategy.first
}

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
micrositeDocumentationUrl := "/vectorpipe/latest/api/#vectorpipe.package" /* Location of Scaladocs */
