import xerial.sbt.Sonatype._
import Dependencies._

name := "vectorpipe"

description := "Import OSM data and output to VectorTiles with GeoTrellis."

lazy val commonSettings = Seq(
  organization := "com.azavea",

  organizationName := "Azavea",

  // We are overriding the default behavior of sbt-git which, by default,
  // only appends the `-SNAPSHOT` suffix if there are uncommitted
  // changes in the workspace.
  version := {
    // Avoid Cyclic reference involving error
    if (git.gitCurrentTags.value.isEmpty || git.gitUncommittedChanges.value)
      git.gitDescribedVersion.value.get + "-SNAPSHOT"
    else
      git.gitDescribedVersion.value.get
  },

  cancelable in Global := true,

  scalaVersion in ThisBuild := Version.scala2_11,

  crossScalaVersions := Seq(Version.scala2_11, Version.scala2_12),

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

  scalacOptions in (Compile, doc) += "-groups",

  /* For Monocle's Lens auto-generation */
  addCompilerPlugin("org.scalamacros" %% "paradise" % "2.1.0" cross CrossVersion.full),

  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
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

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val publishSettings = Seq(
  publishArtifact in Test := false
) ++ sonatypeSettings ++ credentialSettings

lazy val sonatypeSettings = Seq(
  publishMavenStyle := true,

  sonatypeProfileName := "com.azavea",
  sonatypeProjectHosting := Some(GitHubHosting(user="geotrellis", repository="vectorpipe", email="systems@azavea.com")),
  developers := List(
    Developer(id = "jpolchlo", name = "Justin Polchlopek", email = "jpolchlopek@azavea.com", url = url("https://github.com/jpolchlo")),
    Developer(id = "mojodna", name = "Seth Fitzsimmons", email = "seth@mojodna.net", url = url("https://github.com/mojodna"))
  ),
  licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),

  publishTo := sonatypePublishTo.value
)

lazy val credentialSettings = Seq(
  credentials += Credentials(
    "GnuPG Key ID",
    "gpg",
    System.getenv().get("GPG_KEY_ID"),
    "ignored"
  ),

  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "oss.sonatype.org",
    System.getenv().get("SONATYPE_USERNAME"),
    System.getenv().get("SONATYPE_PASSWORD")
  )
)

val vpExtraSettings = Seq(
  libraryDependencies ++= Seq(
    //  gtGeomesa exclude("com.google.protobuf", "protobuf-java") exclude("org.locationtech.geomesa",
    // "geomesa-accumulo-datastore"),
    gtGeotools exclude ("com.google.protobuf", "protobuf-java"),
    awscala,
    scalaj,
    sparkSql % Provided,
    sparkJts,
    gtS3 exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
    gtS3Spark exclude ("com.google.protobuf", "protobuf-java") exclude ("com.amazonaws", "aws-java-sdk-s3"),
    gtSpark exclude ("com.google.protobuf", "protobuf-java"),
    gtVectorTile exclude ("com.google.protobuf", "protobuf-java"),
    decline,
    jaiCore from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar",
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

  test in assembly := {},
  assemblyJarName in assembly := "vectorpipe.jar",

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
  .settings(commonSettings, publishSettings, vpExtraSettings/*, release*/)

/* Benchmarking suite.
 * Benchmarks can be executed by first switching to the `bench` project and then by running:
      jmh:run -t 1 -f 1 -wi 5 -i 5 .*Bench.*
 */
lazy val bench = project
  .in(file("bench"))
  .settings(commonSettings)
  .dependsOn(vectorpipe)
  .enablePlugins(JmhPlugin)
