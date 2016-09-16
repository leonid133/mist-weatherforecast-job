name := "LocalWeatherForecastApp"

version := "1.0"

scalaVersion := "2.10.6"

//val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("1.5.2")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2",
  "org.apache.spark" %% "spark-sql" % "1.5.2",
  "org.apache.spark" %% "spark-hive" % "1.5.2",
  "org.apache.spark" %% "spark-mllib" % "1.5.2",
  "io.hydrosphere" %% "mist" % "0.4.0"
)