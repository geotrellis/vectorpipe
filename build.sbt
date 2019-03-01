name := "vectorpipe"

description := "Import OSM data and output to VectorTiles with GeoTrellis."

import Dependencies._

lazy val commonSettings = Seq(
  organization := "com.azavea",

  organizationName := "Azavea",

  version := Version.vectorpipe,

  cancelable in Global := true,

  scalaVersion in ThisBuild := Version.scala,

  scalacOptions := Seq(
    "-deprecation",
    "-unchecked",
    "-feature",
    "-language:implicitConversions",
    "-language:reflectiveCalls",
    "-language:higherKinds",
    "-language:postfixOps",
    "-language:existentials",
    "-language:experimental.macros",
    "-feature",
    "-Ywarn-dead-code",
    "-Ypartial-unification",
    "-Ypatmat-exhaust-depth", "100"
  ),

  scalacOptions in (Compile, doc) += "-groups",

  /* For Monocle's Lens auto-generation */
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full),

  resolvers ++= Seq(
    Resolver.bintrayRepo("lonelyplanet", "maven"),
    Resolver.bintrayRepo("kwark", "maven"), // Required for Slick 3.1.1.2, see https://github.com/azavea/raster-foundry/pull/1576
    Resolver.bintrayRepo("bkirwi", "maven"), // Required for `decline` dependency
    "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
    "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
    "geosolutions" at "http://maven.geo-solutions.it/",
    "boundless" at "https://repo.boundlessgeo.com/main/",
    "osgeo" at "http://download.osgeo.org/webdav/geotools/",
    "apache.commons.io" at "https://mvnrepository.com/artifact/commons-io/commons-io"
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

  updateOptions := updateOptions.value.withGigahorse(false),

  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },

  assemblyMergeStrategy in assembly := {
    case "reference.conf" | "application.conf"  => MergeStrategy.concat
    case PathList("META-INF", xs@_*) =>
      xs match {
        case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
        // Concatenate everything in the services directory to keep GeoTools happy.
        case ("services" :: _ :: Nil) =>
          MergeStrategy.concat
        // Concatenate these to keep JAI happy.
        case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
          MergeStrategy.concat
        case (name :: Nil) => {
          // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
          if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
            MergeStrategy.discard
          else
            MergeStrategy.first
        }
        case _ => MergeStrategy.first
      }
    case _ => MergeStrategy.first
  }
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

val vpExtraSettings = Seq(
  libraryDependencies ++= Seq(
    //  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa",
    // "geomesa-accumulo-datastore"),
    gtGeotools exclude ("com.google.protobuf", "protobuf-java"),
    "com.github.seratch" %% "awscala" % "0.6.1",
    "org.scalaj" %% "scalaj-http" % "2.3.0",
    sparkHive % Provided,
    sparkJts,
    gtS3 exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
    gtSpark exclude ("com.google.protobuf", "protobuf-java"),
    gtVectorTile exclude ("com.google.protobuf", "protobuf-java"),
    decline,
    jaiCore,
    gtVector,
    cats,
    scalactic,
    scalatest,
    circeCore,
    circeGeneric,
    circeExtras,
    circeParser,
    circeOptics,
    circeJava8,
    circeYaml,
    "com.softwaremill.macmemo" %% "macros" % "0.4",
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.340" % Provided
  ),

  Test / fork := true,
  Test / baseDirectory := (baseDirectory.value).getParentFile,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF")

)

// /* Microsite Settings
//  *
//  * To generate the microsite locally, use `sbt makeMicrosite`.
//  * To publish the site to Github, use `sbt publishMicrosite`.
//  *
//  * Spark deps must not be marked `provided` while doing these, or you will get errors.
//  */

// enablePlugins(MicrositesPlugin)
// enablePlugins(SiteScaladocPlugin)

// micrositeName := "VectorPipe"
// micrositeDescription := "Convert Vector data into VectorTiles"
// micrositeAuthor := "GeoTrellis Team at Azavea"
// micrositeGitterChannel := false
// micrositeOrganizationHomepage := "https://www.azavea.com/"
// micrositeGithubOwner := "geotrellis"
// micrositeGithubRepo := "vectorpipe"
// micrositeBaseUrl := "/vectorpipe"
// micrositeDocumentationUrl := "/vectorpipe/latest/api/#vectorpipe.package" /* Location of Scaladocs */

/* Main project */
lazy val vectorpipe = project
  .in(file("."))
  .settings(commonSettings, vpExtraSettings, release)

/* Benchmarking suite.
 * Benchmarks can be executed by first switching to the `bench` project and then by running:
      jmh:run -t 1 -f 1 -wi 5 -i 5 .*Bench.*
 */
lazy val bench = project
  .in(file("bench"))
  .settings(commonSettings)
  .dependsOn(vectorpipe)
  .enablePlugins(JmhPlugin)




// assemblyShadeRules in assembly := {
//   val shadePackage = "com.azavea.shaded.demo"
//   Seq(
//     ShadeRule.rename("com.google.common.**" -> s"$shadePackage.google.common.@1")
//       .inLibrary("com.azavea.geotrellis" %% "geotrellis-cassandra" % Version.geotrellis).inAll,
//     ShadeRule.rename("io.netty.**" -> s"$shadePackage.io.netty.@1")
//       .inLibrary("com.azavea.geotrellis" %% "geotrellis-hbase" % Version.geotrellis).inAll,
//     ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
//       .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
//     ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
//       .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % Version.geotrellis).inAll
//   )
// }
