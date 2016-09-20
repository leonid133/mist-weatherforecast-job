name := "LocalWeatherForecastApp"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("2.0.0")

resolvers += "Typesafe Repository" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "io.hydrosphere" %% "mist" % "0.4.1-SNAPSHOT"
)
