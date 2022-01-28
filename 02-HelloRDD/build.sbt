name := "HelloRDD"
organization := "com.spark.learnings"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"
autoScalaLibrary := false
val sparkVersion = "3.0.0-preview2"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)

libraryDependencies ++= sparkDependencies

