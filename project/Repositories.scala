import sbt._

object Repositories {
  val apacheSnapshots  = "apache-snapshots" at "https://repository.apache.org/content/repositories/snapshots/"
  val eclipseReleases  = "eclipse-releases" at "https://repo.eclipse.org/content/groups/releases"
  val eclipseSnapshots = "eclipse-snapshots" at "https://repo.eclipse.org/content/groups/snapshots"
  val osgeoReleases    = "osgeo-releases" at "https://repo.osgeo.org/repository/release/"
  val geosolutions     = "geosolutions" at "https://maven.geo-solutions.it/"
  val jitpack          = "jitpack" at "https://jitpack.io" // for https://github.com/everit-org/json-schema
  val ivy2Local        = Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
  val mavenLocal       = Resolver.mavenLocal
  val maven            = DefaultMavenRepository
  val local            = Seq(ivy2Local, mavenLocal)
  val external         = Seq(osgeoReleases, maven, eclipseReleases, geosolutions, jitpack, apacheSnapshots, eclipseSnapshots)
  val all              = external ++ local
}
