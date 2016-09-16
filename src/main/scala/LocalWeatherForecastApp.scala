import java.nio.file.Paths
import java.nio.file.Files
import java.util.Date

import io.hydrosphere.mist.MistJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.json4s._
import org.json4s.JsonDSL._
import java.io._

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

case class NearPoint(var usaf: Int, var wban: Int, var name: String, var lat: Float, var lng: Float, var year: Int)

object LocalWeatherForecastApp extends MistJob {

   override def doStuff(contextSQL: SQLContext, parameters: Map[String, Any]): Map[String, Any] = {

    val context = contextSQL.sparkContext

    parameters.foreach(f => println(f.toString()))
    val points = parameters("points").asInstanceOf[List[Map[String, String]]]
    val legs = parameters("legs").asInstanceOf[Map[String, Map[String, Any]]]

    val duration = legs("duration").asInstanceOf[Map[String, Any]]
    val durationValue = duration("value").asInstanceOf[BigInt]
    val distance = legs("distance").asInstanceOf[Map[String, Any]]
    val distanceValue = distance("value").asInstanceOf[BigInt]

    val myTz = DateTimeZone.getDefault()
    val now = new Date()
    val utcString = new DateTime(now).withZone(DateTimeZone.UTC).toString()

     val pointsIt = points.iterator
     var resList = new ListBuffer[Result]()

     val r = scala.util.Random

     for (idx <- 1 to points.length) {
       val currentPoint = pointsIt.next ()
       val timeInPoint = new DateTime (now).withZone(DateTimeZone.UTC).plusSeconds(((durationValue / points.length) * (points.length - idx - 1) ).toInt)
       println(timeInPoint.toString())
       val result = new Result (currentPoint, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextInt(30)).toInt, timeInPoint.toString () )
       resList += result
     }

     val answer = new ResultList(resList.toList)

     val isd_hystory = context.textFile("source/noaa/isd-history.csv")
     for(answerpoint <- answer.results) {
       val latfind = answerpoint.point("lat").toFloat
       val lngfind = answerpoint.point("lng").toFloat

       var nearPointStations = ArrayBuffer[NearPoint]()
         for(stationIter <- 1 to 5){
           nearPointStations += new NearPoint(0, 0, "", (180.0).toFloat, (360.0).toFloat, 0)
         }

       for (line <- isd_hystory.collect().drop(1)) {
         val rows = line.split(",").toList.zipWithIndex
         val usaf = rows.filter(row => row._2 == 0).head._1.replaceAll("\"", "").toInt
         val wban = rows.filter(row => row._2 == 1).head._1.replaceAll("\"", "").toInt
         val stationName: String = rows.filter(row => row._2 == 2).head._1
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

         var year = new DateTime (now).withZone(DateTimeZone.UTC).getYear().toInt
         for{stationIter <- nearPointStations}{
           var latDistance = math.abs(stationIter.lat - latfind).toFloat
           if (latDistance > 90.0) latDistance = latDistance - 90.0.toFloat

           var latDistanceNew = math.abs(lat - latfind).toFloat
           if (latDistanceNew > 90.0) latDistanceNew = latDistanceNew - 90.0.toFloat

           if (latDistance >= latDistanceNew) {
             var lonDistance = math.abs(stationIter.lng - lngfind).toFloat
             if (lonDistance > 180.0) lonDistance = lonDistance - 180.0.toFloat

             var lonDistanceNew = math.abs(lon - lngfind).toFloat
             if (lonDistanceNew > 180.0) lonDistanceNew = lonDistanceNew - 180.0.toFloat
             if (lonDistance >= lonDistanceNew) {
               if (Files.exists(Paths.get(s"source/noaa/${year}/${usaf}-${wban}-${year}.gz"))) {
                 stationIter.usaf = usaf
                 stationIter.wban = wban
                 stationIter.name = stationName
                 stationIter.lat = lat
                 stationIter.lng = lon
                 stationIter.year = year

               }
             }
           }
           year -= 1
         }
       }

       val srcFile = {
         try {
           var files = ArrayBuffer[RDD[String]]()
           for {stationIter <- nearPointStations} {
             println(stationIter.toString)
             files +=
               context.textFile(s"source/noaa/${stationIter.year}/${stationIter.usaf}-${stationIter.wban}-${stationIter.year}.gz")
           }
           context.union(files)
         }
         catch {
           case _ => context.textFile("source/null")
         }
       }

//       var xk = ArrayBuffer[Double]()
//       for (k <- 0 to srcFile.collect().length) {
//         xk += 0.0
//       }
//
//       println(srcFile.collect().length, xk.size)
//       var n = 0

       var deltaTime = (new DateTime(answerpoint.datetime).withZone(DateTimeZone.UTC)).getMillis
       val pwt = new PrintWriter(new File("source/temp.txt"))
//       var yesterdayTemp = 0.0
//       var tempInMorning = 0.0

       for (line <- srcFile.collect()) {
         //println(line)
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

         val deltaTimeNew = (utcTimeStation.getMillis - (new DateTime(answerpoint.datetime).withZone(DateTimeZone.UTC)).getMillis) / (1000 * 60 * 60)
         if (math.abs(deltaTimeNew) < math.abs(deltaTime)) {
           //println(deltaTimeNew, deltaTime)
           answerpoint.temperature = airTemperature.toInt
           deltaTime = deltaTimeNew
           //println(s"currT ${ (new DateTime(answerpoint.datetime).withZone(DateTimeZone.UTC))}  utcTime ${utcTimeStation.toString()}, lat ${latitude}, lon ${longitude}, temp ${airTemperature}, pressure ${seaLevelPressure}")
         }

         if (airTemperature.toInt < 30 && airTemperature.toInt > -10) {

           val dataNorm = s"${(airTemperature.toInt + 10)} " +
             s"1:${geoPointDate.substring(0, 4).toDouble/2016.0} " +
             s"2:${geoPointDate.substring(4, 6).toDouble/12.0} " +
             s"3:${geoPointDate.substring(6, 8).toDouble/31.0} " +
             s"4:${geoPointTime.substring(0, 2).toDouble/24.0} " +
             s"5:${geoPointTime.substring(2, 4).toDouble/60.0}"

           //println(dataNorm)

           pwt.write(s"${dataNorm} \r\n")

//         for (k <- 1 to line.length) {
//           xk(k) = xk(k) + (math.exp(-2.0 * 3.14 * k * (n / line.length)) * (airTemperature.toInt).toDouble) * (1.0 / line.length.toDouble)
//         }

         }
         else {
//         for (k <- 1 to line.length) {
//           xk(k) = xk(k) + (math.exp(-2.0 * 3.14 * k * (n / line.length)) * yesterdayTemp.toInt.toDouble) * (1.0 / line.length.toDouble)
//         }
         }

         //n = n + 1
       }
       pwt.close()

//       val pw = new PrintWriter(new File("xk.txt" ))
//       xk.map(x => pw.write(s"${x.toInt.toString} \r\n"))
//       pw.close

//       xk.map(x => println(x.toString))

       val data = MLUtils.loadLibSVMFile(contextSQL.sparkContext, "source/temp.txt")

       val dataFrame = contextSQL.createDataFrame(data)

       val splits = dataFrame.randomSplit(Array(0.8, 0.2), seed = 1234L)
       val train = splits(0)
       val test = splits(1)
       // specify layers for the neural network:
       val layers = Array[Int](5, 1000, 2000, 40)

       val trainer = new MultilayerPerceptronClassifier()
         .setLayers(layers)
         .setBlockSize(128)
         .setSeed(1234L)
         .setMaxIter(300)
       val model = trainer.fit(train)

       val result = model.transform(test)
       result.select("prediction", "label", "features").show(250)
       //     result.schema.printTreeString()
       //     result.select("features").foreach(println)
       val predictionAndLabels = result.select("prediction", "label")
       val evaluator = new MulticlassClassificationEvaluator()
         .setMetricName("precision")
       println("Precision:" + evaluator.evaluate(predictionAndLabels))
       val featureReq = Seq((1,
         Vectors.dense( answerpoint.datetime.substring(0, 4).toDouble/2016.0,
                        answerpoint.datetime.substring(5, 7).toDouble/12.0,
                        answerpoint.datetime.substring(8, 10).toDouble/31.0,
                        answerpoint.datetime.substring(11, 13).toDouble/24.0,
                        answerpoint.datetime.substring(14, 16).toDouble/60.0)))
       val requestData = contextSQL.createDataFrame(featureReq).toDF("label", "features")

       val prediction = model.transform(requestData)

       prediction.select("prediction", "label", "features").show(5)
       prediction.schema.printTreeString()

       //prediction.select("prediction").foreach(println)
       //val predictedTemperature = prediction.select("prediction")

       //answerpoint.temperature = predictedTemperature.rdd.first().getDouble(0).toInt -10

       println(answerpoint.temperature)
     }
     val obj: JObject =
       ( "parameters" -> answer.results.map {w =>
         ("point" ->
           ("lat" -> w.point("lat").toFloat) ~
           ("lng" -> w.point("lng").toFloat)
         ) ~
         ("sun" -> w.sun) ~
         ("cloud" -> w.cloud) ~
         ("rain" -> w.rain) ~
         ("temperature" -> w.temperature) ~
         ("datetime" -> w.datetime)
     } )

    //println(compact(render(obj)))

    Map("result" -> obj)

  }
}