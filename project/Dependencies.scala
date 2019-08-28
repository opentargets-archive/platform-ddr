import sbt._

object Dependencies {
  lazy val scalaLoggingDep = "ch.qos.logback" % "logback-classic" % "1.2.3"
  lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  lazy val scalaMiniTestSeq = Seq(
    "io.monix" %% "minitest" % "2.2.1" % "test",
    "io.monix" %% "minitest-laws" % "2.2.1" % "test"
  )
  lazy val scopt = "com.github.scopt" %% "scopt" % "3.7.0"
  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.0"
  lazy val sparkSeq = Seq(
    "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
    "org.apache.spark" %% "spark-core" % "2.4.3",
    "org.apache.spark" %% "spark-sql" % "2.4.3",
    "org.apache.spark" %% "spark-graphx" % "2.4.3",
    "org.apache.spark" %% "spark-mllib" % "2.4.3"
  )
  lazy val ammonite = "com.lihaoyi" % "ammonite" % "1.6.8" % "test" cross CrossVersion.full
  lazy val ammoniteDeps = Seq(
    "com.github.pathikrit" %% "better-files" % "3.8.0",
    "com.typesafe.play" %% "play-json" % "2.7.3",
    "com.typesafe" % "config" % "1.3.4"
  )
}
