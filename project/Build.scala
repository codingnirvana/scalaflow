import sbt._
import Keys._

object ScalaFlowBuild extends Build {

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    // also search local maven repo
    resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",

    // project settings
    version := "0.1.0",
    organization := "me.juhanlol",
    scalaVersion := "2.10.4",
    // dependencies
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.10.4",
      "com.twitter" %% "chill" % "0.5.1",
      "org.joda" % "joda-convert" % "1.2",
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "0.3.141216",
      // test dependencies
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.3" % "test",
      "junit" % "junit" % "4.9" % "test"
    )
  )

  lazy val project = Project("scala-flow", file("."),
    settings = buildSettings)
}
