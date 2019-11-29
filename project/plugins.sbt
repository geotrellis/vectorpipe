addSbtPlugin("com.47deg"  % "sbt-microsites" % "0.7.4")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.2.27")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.5")

addSbtPlugin("io.crashbox" % "sbt-gpg" % "0.2.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.0")
