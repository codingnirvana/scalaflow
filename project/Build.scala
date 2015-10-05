import sbt._
import Keys._

object ScalaFlowBuild extends Build {

  lazy val buildSettings = Defaults.defaultSettings ++ Seq(
    // also search local maven repo
    resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository",
    resolvers += Resolver.sonatypeRepo("snapshots"),
    resolvers += "Conjars" at "http://conjars.org/repo",
    resolvers += "Cloudera" at "https://repository.cloudera.com/cloudera/public",

    // project settings
    version := "0.1.0",
    organization := "me.juhanlol",
    scalaVersion := "2.10.4",
    // dependencies
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % "2.10.4",
      "com.twitter" %% "chill" % "0.5.1",
      "org.joda" % "joda-convert" % "1.2",
      // example dependencies
      "com.google.http-client" % "google-http-client-jackson2" % "1.19.0" exclude ("com.google.guava", "guava-jdk5"),
      "com.google.apis" % "google-api-services-storage" % "v1-rev25-1.19.1" exclude("com.google.guava","guava-jdk5"),
      "com.google.apis" % "google-api-services-bigquery" % "v2-rev187-1.19.1" exclude ("com.google.guava", "guava-jdk5"),
      "com.google.guava" % "guava" % "18.0",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.4.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.2",
      "com.google.cloud.dataflow" % "google-cloud-dataflow-java-sdk-all" % "1.1.0" exclude("org.slf4j","slf4j-jdk14") ,
      "in.ashwanthkumar" % "scalding-dataflow_2.10" % "1.0.23-SNAPSHOT",
//      "com.cloudera.dataflow.spark" % "spark-dataflow" % "0.4.2",
//      "org.apache.spark" % "spark-core_2.10" % "1.3.1",

      // test dependencies
      "org.scalatest" %% "scalatest" % "2.2.1" % "test",
      "org.hamcrest" % "hamcrest-all" % "1.3" % "test",
      "junit" % "junit" % "4.9" % "test"
    )
  )

  lazy val project = Project("scala-flow", file("."),
    settings = buildSettings)
}
