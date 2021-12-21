import xerial.sbt.Sonatype._
import Dependencies._

lazy val commonSettings = Seq(
  scalaVersion := Version.scala.head,
  crossScalaVersions := Version.scala,

  description := "Import OSM data and output to VectorTiles with GeoTrellis.",
  organization := "com.azavea.geotrellis",
  organizationName := "GeoTrellis",
  organizationHomepage := Some(new URL("https://geotrellis.io/")),
  homepage := Some(url("https://github.com/geotrellis/vectorpipe")),
  versionScheme := Some("semver-spec"),

  developers := List(
    Developer(id = "jpolchlo", name = "Justin Polchlopek", email = "jpolchlopek@azavea.com", url = url("https://github.com/jpolchlo")),
    Developer(id = "mojodna", name = "Seth Fitzsimmons", email = "seth@mojodna.net", url = url("https://github.com/mojodna"))
  ),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  cancelable in Global := true,
  //publishArtifact in Test := false,

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
    "-Ypatmat-exhaust-depth", "100",
    "-Ywarn-unused-import"
  ),

  Compile / doc / scalacOptions += "-groups",
  Compile / console / scalacOptions ~= { _.filterNot(Set("-Ywarn-unused-import", "-Ywarn-unused:imports")) },

  /* For Monocle's Lens auto-generation */
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full),

  externalResolvers := Repositories.all,

  updateOptions := updateOptions.value.withGigahorse(false),

  shellPrompt := { s => Project.extract(s).currentProject.id + " > " },

  assembly / assemblyMergeStrategy := {
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

val vpExtraSettings = Seq(
  libraryDependencies ++= Seq(
    //  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa",
    // "geomesa-accumulo-datastore"),
    gtGeotools exclude ("com.google.protobuf", "protobuf-java"),
    awscala,
    scalaj,
    sparkHive % Provided,
    sparkSql % Provided,
    sparkJts,
    gtS3 exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
    gtS3Spark exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
    gtSpark exclude ("com.google.protobuf", "protobuf-java"),
    gtVectorTile exclude ("com.google.protobuf", "protobuf-java"),
    decline,
    //jaiCore from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar",
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
    "com.amazonaws" % "aws-java-sdk-s3" % "1.11.518" % Provided
  ),

  dependencyOverrides ++= {
    val deps = Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7"
    )
    CrossVersion.partialVersion(scalaVersion.value) match {
      // if Scala 2.12+ is used
      case Some((2, scalaMajor)) if scalaMajor >= 12 => deps
      case _ => deps :+ "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
    }
  },

  assembly / test := {},
  assembly / assemblyJarName := "vectorpipe.jar",

  Test / fork := true,
  Test / baseDirectory := (baseDirectory.value).getParentFile,
  Test / parallelExecution := false,
  Test / testOptions += Tests.Argument("-oDF"),

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
  .settings(moduleName := "vectorpipe", commonSettings, vpExtraSettings)

/* Benchmarking suite.
 * Benchmarks can be executed by first switching to the `bench` project and then by running:
      jmh:run -t 1 -f 1 -wi 5 -i 5 .*Bench.*
 */
lazy val bench = project
  .in(file("bench"))
  .settings(commonSettings)
  .dependsOn(vectorpipe)
  .enablePlugins(JmhPlugin)
