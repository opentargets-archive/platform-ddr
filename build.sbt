import Dependencies._

val buildResolvers = Seq(
    "Local Maven Repository"    at "file://"+Path.userHome.absolutePath+"/.m2/repository",
    "Maven repository"          at "http://download.java.net/maven/2/",
    "Typesafe Repo"             at "http://repo.typesafe.com/typesafe/releases/",
    "Sonatype Snapshots"        at "http://oss.sonatype.org/content/repositories/snapshots",
    "Sonatype Releases"         at "http://oss.sonatype.org/content/repositories/releases"
  )

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "io.opentargets",
      scalaVersion := "2.11.12",
      version      := "0.0.6"
    )),
    name := "io-opentargets-platform-ddr",
    // libraryDependencies += scalaTest % Test,

    // resolvers += Resolver.mavenLocal,
    // resolvers += Resolver.sonatypeRepo("releases"),
    resolvers ++= buildResolvers,

    // from Dependencies.scala
    libraryDependencies += scalaCheck,
    libraryDependencies += scopt,
    libraryDependencies ++= scalaMiniTestSeq,
    libraryDependencies ++= sparkSeq,
    libraryDependencies += scalaLoggingDep,
    libraryDependencies += scalaLogging,

    testFrameworks += new TestFramework("minitest.runner.Framework"),

    assemblyMergeStrategy in assembly := {
      case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
      case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
      case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
      case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
      case PathList("org", "apache", xs @ _*) => MergeStrategy.last
      case PathList("com", "google", xs @ _*) => MergeStrategy.last
      case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
      case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
      case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
      case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.last
      case "about.html" => MergeStrategy.rename
      case "overview.html" => MergeStrategy.rename
      case "plugin.properties" => MergeStrategy.last
      case "log4j.properties" => MergeStrategy.last
      case "git.properties" => MergeStrategy.last
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
      }
  )
