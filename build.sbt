name := "imdb-rankings"

version := "0.1"

scalaVersion := "2.12.10"

val ScalaTestVersion = "3.0.8"
val sparkVersion = "2.2.0"

scalaVersion := "2.11.8"

mainClass := Some("com.moyano.imdb.AppImdb")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.scalactic" %% "scalactic" % ScalaTestVersion % Test,
  "org.scalatest" %% "scalatest" % ScalaTestVersion % Test
)