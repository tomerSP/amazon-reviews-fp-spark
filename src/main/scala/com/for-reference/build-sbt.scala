name := "amazon-reviews-fp-spark"

version := "1.0.0"

scalaVersion := "2.12.17"

// Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",

  // JSON parsing
  "org.json4s" %% "json4s-jackson" % "4.0.6",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.scalactic" %% "scalactic" % "3.2.15" % Test
)

// Assembly plugin configuration for creating fat JAR
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Compiler options for functional programming
scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)

// Test configuration
Test / parallelExecution := false
Test / fork := true
Test / javaOptions ++= Seq("-Xmx2G")

// Run configuration
run / fork := true
run / javaOptions ++= Seq("-Xmx4G")
