name := "imdb-rankings"

version := "0.1"

scalaVersion := "2.13.1"

val ScalaTestVersion = "3.0.8"

libraryDependencies ++= Set(
  "org.apache.spark" %% "spark-sql" % "2.4.4",
  "org.scalactic" %% "scalactic" % ScalaTestVersion % Test,
  "org.scalatest" %% "scalatest" % ScalaTestVersion % Test
)