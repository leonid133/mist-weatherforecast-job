import java.nio.file.Paths
import java.nio.file.Files
import java.util.Date

import io.hydrosphere.mist.MistJob
import org.apache.spark.sql._
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.json4s._
import org.json4s.JsonDSL._
import java.io._
import java.net.URI

import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.mapdb.{DBMaker, Serializer}
import org.apache.commons.lang.SerializationUtils

object LocalWeatherForecastTrainer extends MistJob {

  override def doStuff(sparkSession: SparkSession, parameters: Map[String, Any]): Map[String, Any] = {

    val hdfshost = "hdfs://172.32.1.53:9000"
    val source = s"${hdfshost}/weather" // source/noaa/

    val contextSQL = sparkSession.sqlContext
    val context = sparkSession.sparkContext

    val myTimeZone = DateTimeZone.getDefault()
    val nowDate = new Date()
    val nowDateUtcString = new DateTime(nowDate).withZone(DateTimeZone.UTC).toString()

    val isdHystory = context.textFile(s"${source}/isd-history.csv")

    var nearPointStations = ArrayBuffer[NearPoint]()
     for(stationIter <- 1 to 5){
       nearPointStations += new NearPoint("", "", "", (180.0).toFloat, (360.0).toFloat, 0)
     }

    isdHystory.collect().drop(1).map { line =>
     val rows = line.split(",").toList.zipWithIndex

     val usaf = rows.filter(row => row._2 == 0).head._1.replaceAll("\"", "")
     val wban = rows.filter(row => row._2 == 1).head._1.replaceAll("\"", "")
     val stationName: String = rows.filter(row => row._2 == 2).head._1.replaceAll("\"", "")
     val latStr = rows.filter(row => row._2 == 6).head.toString().replaceAll("[\"()]", "").replaceFirst(",6", "")

     val lat = if (latStr.length > 0) {
       latStr.substring(0, 1) match {
         case "-" => -latStr.replaceAll("[-]", "").toFloat
         case _ => latStr.replaceAll("[+]", "").toFloat
       }
     }
     else 0.0.toFloat

     val lonStr = rows.filter(row => row._2 == 7).head.toString().replaceAll("[\"()]", "").replaceFirst(",7", "")
     val lon = if (lonStr.length > 0) {
       lonStr.substring(0, 1) match {
         case "-" => -lonStr.replaceAll("[-]", "").toFloat
         case _ => lonStr.replaceAll("[+]", "").toFloat
       }
     } else 0.0.toFloat

     var year = new DateTime (nowDate).withZone(DateTimeZone.UTC).getYear().toInt

     for{stationIter <- nearPointStations}{
       val conf = context.hadoopConfiguration
       val fs = org.apache.hadoop.fs.FileSystem.get(new URI(hdfshost), conf)
       val exists = fs.exists(new org.apache.hadoop.fs.Path(s"${source}/${year}/${usaf}-${wban}-${year}.gz"))

       if(exists) {
         stationIter.usaf = usaf
         stationIter.wban = wban
         stationIter.name = stationName
         stationIter.lat = lat
         stationIter.lng = lon
         stationIter.year = year
       }
       year -= 1
     }
      val conf = context.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(new URI(hdfshost), conf)
      val exists = fs.exists(new org.apache.hadoop.fs.Path(s"${source}/${year}/${usaf}-${wban}-${year}.gz"))

      if (exists) {
        val srcFile = {
          try {
            var files = ArrayBuffer[RDD[String]]()
            for {stationIter <- nearPointStations} {
              println(stationIter.toString)

              val conf = context.hadoopConfiguration
              val fs = org.apache.hadoop.fs.FileSystem.get(new URI(hdfshost), conf)
              val exists = fs.exists(new org.apache.hadoop.fs.Path(s"${source}/${stationIter.year}/${stationIter.usaf}-${stationIter.wban}-${stationIter.year}.gz"))

              if(exists)
                files += context.textFile(s"${source}/${stationIter.year}/${stationIter.usaf}-${stationIter.wban}-${stationIter.year}.gz")
            }
            context.union(files)
          }
          catch {
            case _: Throwable => {
              (new PrintWriter(new File(s"null"))).close()
              context.textFile(s"null")
            }
          }
        }

//        val pwt = new PrintWriter(new File(s"temp_teach.txt"))
        val temperatures = ArrayBuffer[String]()

        srcFile.collect().map { line =>

          val numDataSection = line.substring(0, 4)
          val usaf = line.substring(4, 10)
          val wban = line.substring(10, 15)
          val geoPointDate = line.substring(15, 23)
          val geoPointTime = line.substring(23, 27)
          val geoPointSourceFlag = line.substring(27, 28)
          val latitude = line.substring(28, 34).toFloat / 1000.0
          val longitude = line.substring(34, 41).toFloat / 1000.0
          val reportType = line.substring(41, 46)
          val elevationDimension = line.substring(46, 51)
          val callLetterIdentiefer = line.substring(51, 56)
          val qualityControlProcessName = line.substring(56, 60)
          val directionAngle = line.substring(60, 63)
          val directionQualityCode = line.substring(63, 64)
          val typeCode = line.substring(64, 65)
          val speedRate = line.substring(65, 69)
          val speedQualityCode = line.substring(69, 70)
          val cellingHeightDimension = line.substring(70, 75)
          val cellingQualityCode = line.substring(75, 76)
          val cellingDeterminationCode = line.substring(76, 77)
          val okCode = line.substring(77, 78)
          val distanceDimension = line.substring(78, 84)
          val distanceQualityCode = line.substring(84, 85)
          val variablilityCode = line.substring(85, 86)
          val qualityVariabilityCode = line.substring(86, 87)
          val airTemperature = line.substring(87, 92).toFloat / 10.0
          val airTemperatureQualityCode = line.substring(92, 93)
          val dewPointTemperature = line.substring(93, 98)
          val dewPointQualityCode = line.substring(98, 99)
          val seaLevelPressure = line.substring(99, 104).toFloat / 10.0
          val seaLevelPressureQualityCode = line.substring(104, 105)

          val utcTimeStationString = s"${geoPointDate.substring(0, 4)}-${geoPointDate.substring(4, 6)}-${geoPointDate.substring(6, 8)}T${geoPointTime.substring(0, 2)}:${geoPointTime.substring(2, 4)}:00Z"
          val utcTimeStation = new DateTime(utcTimeStationString).withZone(DateTimeZone.UTC)

          if (airTemperature.toInt < 50 && airTemperature.toInt > -50) {

//            val dataNorm = s"${((airTemperature / 4).toInt + 13).toDouble} " +
//              s"1:${geoPointDate.substring(0, 4).toDouble / 2016.0} " +
//              s"2:${geoPointDate.substring(4, 6).toDouble / 12.0} " +
//              s"3:${geoPointDate.substring(6, 8).toDouble / 31.0} " +
//              s"4:${geoPointTime.substring(0, 2).toDouble / 24.0} " +
//              s"5:${geoPointTime.substring(2, 4).toDouble / 60.0} "


            temperatures += s"${((airTemperature / 4).toInt + 13).toDouble} " +
                            s"1:${geoPointDate.substring(0, 4).toDouble / 2016.0} " +
                            s"2:${geoPointDate.substring(4, 6).toDouble / 12.0} " +
                            s"3:${geoPointDate.substring(6, 8).toDouble / 31.0} " +
                            s"4:${geoPointTime.substring(0, 2).toDouble / 24.0} " +
                            s"5:${geoPointTime.substring(2, 4).toDouble / 60.0} \r\n"

//            pwt.write(s"${dataNorm} \r\n")

          }
        }

//        pwt.close()

        if (!Files.exists(Paths.get(s"${nearPointStations.head.name}/data/_SUCCESS"))) {
//          if (Files.exists(Paths.get("temp_teach.txt"))) {
//            val dataFrame = contextSQL.read.format("libsvm")
//              .load(s"temp_teach.txt")


            import sparkSession.implicits._
            val pwt = new PrintWriter(new File("temp_teach.txt"))
            pwt.write(temperatures.toString())
            pwt.flush()
            pwt.close()
            val dataFrame = contextSQL.read.format("libsvm").load("temp_teach.txt")
          //val requestData = contextSQL.createDataFrame(temperatures).toDF("label", "features")

            dataFrame.show(5)

            val splits = dataFrame.randomSplit(Array(0.9, 0.1), seed = 1234L)
            val train = splits(0)
            val test = splits(1)
            // specify layers for the neural network:
            val layers = Array[Int](5, 42, 26)


            val trainer = new MultilayerPerceptronClassifier()
              .setLayers(layers)
              .setBlockSize(64)
              .setSeed(1234L)
              .setMaxIter(300)


            val model = trainer.fit(train)

            model.save(s"${nearPointStations.head.name}")

            val result = model.transform(test)

            result.show(15)

            val predictionAndLabels = result.select("prediction", "label")
            val evaluator = new MulticlassClassificationEvaluator()
              .setMetricName("accuracy")
            println("Accuracy:" + evaluator.evaluate(predictionAndLabels))
//          }
        }
      }
    }

    Map("result" -> "42")
  }
}