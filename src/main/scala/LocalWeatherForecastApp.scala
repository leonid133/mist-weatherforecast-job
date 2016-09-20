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

import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD

import org.mapdb.{DBMaker, Serializer}
import org.apache.commons.lang.SerializationUtils

case class NearPoint(var usaf: Int, var wban: Int, var name: String, var lat: Float, var lng: Float, var year: Int)

object LocalWeatherForecastApp extends MistJob {

  override def doStuff(sparkSession: SparkSession, parameters: Map[String, Any]): Map[String, Any] = {

    val contextSQL = sparkSession.sqlContext
    val context = sparkSession.sparkContext

    parameters.foreach(f => println(f.toString()))
    val points = parameters("points").asInstanceOf[List[Map[String, String]]]
    val legs = parameters("legs").asInstanceOf[Map[String, Map[String, Any]]]

    val duration = legs("duration").asInstanceOf[Map[String, Any]]
    val durationValue = duration("value").asInstanceOf[BigInt]
    val distance = legs("distance").asInstanceOf[Map[String, Any]]
    val distanceValue = distance("value").asInstanceOf[BigInt]

    val myTimeZone = DateTimeZone.getDefault()
    val nowDate = new Date()
    val nowDateUtcString = new DateTime(nowDate).withZone(DateTimeZone.UTC).toString()

     val pointsIterator = points.iterator
     var resultList = new ListBuffer[Result]()

     val r = scala.util.Random

     for (idx <- 1 to points.length) {
       val currentPoint = pointsIterator.next ()
       val timeInPoint = new DateTime (nowDate).withZone(DateTimeZone.UTC).plusSeconds(((durationValue / points.length) * (points.length - idx - 1) ).toInt)
       println(timeInPoint.toString())
       val result = new Result (currentPoint, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextInt(30)).toInt, timeInPoint.toString (), "" )
       resultList += result
     }

     val answerResultList = new ResultList(resultList.toList)

     val isdHystory = context.textFile("source/noaa/isd-history.csv")
     for(answerpoint <- answerResultList.results) {
       val latfind = answerpoint.point("lat").toFloat
       val lngfind = answerpoint.point("lng").toFloat

       var nearPointStations = ArrayBuffer[NearPoint]()
         for(stationIter <- 1 to 5){
           nearPointStations += new NearPoint(0, 0, "", (180.0).toFloat, (360.0).toFloat, 0)
         }

       for (line <- isdHystory.collect().drop(1)) {
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

         var year = new DateTime (nowDate).withZone(DateTimeZone.UTC).getYear().toInt
         for{stationIter <- nearPointStations}{
           var latDistanceNewStation = math.abs(lat - latfind).toFloat
           var lonDistanceNewStation = math.abs(lon - lngfind).toFloat
           if (latDistanceNewStation > 180.0) latDistanceNewStation = latDistanceNewStation - 180.0.toFloat
           if (lonDistanceNewStation > 360.0) lonDistanceNewStation = lonDistanceNewStation - 360.0.toFloat

           var latDistanceStationPoint = math.abs(stationIter.lat - latfind).toFloat
           if (latDistanceStationPoint > 90.0) latDistanceStationPoint = latDistanceStationPoint - 90.0.toFloat
           var lonDistanceStationPoint = math.abs(stationIter.lng - lngfind).toFloat
           if (lonDistanceStationPoint > 180.0) lonDistanceStationPoint = lonDistanceStationPoint - 180.0.toFloat

           if ( math.pow((math.pow(latDistanceStationPoint, 2) + math.pow(lonDistanceStationPoint, 2)), 0.5) >=
                math.pow((math.pow(latDistanceNewStation, 2) + math.pow(lonDistanceNewStation, 2)), 0.5)) {

               if (Files.exists(Paths.get(s"source/noaa/${year}/${usaf}-${wban}-${year}.gz"))) {
                 stationIter.usaf = usaf
                 stationIter.wban = wban
                 stationIter.name = stationName
                 stationIter.lat = lat
                 stationIter.lng = lon
                 stationIter.year = year
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
             answerpoint.stationName = stationIter.name
             files +=
               context.textFile(s"source/noaa/${stationIter.year}/${stationIter.usaf}-${stationIter.wban}-${stationIter.year}.gz")
           }
           context.union(files)
         }
         catch {
           case _ : Throwable => context.textFile("source/null")
         }
       }

       var deltaTime = (new DateTime(answerpoint.datetime).withZone(DateTimeZone.UTC)).getMillis
       val pwt = new PrintWriter(new File("source/temp.txt"))

       for (line <- srcFile.collect()) {

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
           answerpoint.temperature = airTemperature.toInt
           deltaTime = deltaTimeNew
         }

         if (airTemperature.toInt < 50 && airTemperature.toInt > -50) {

           val dataNorm = s"${((airTemperature/4).toInt + 13).toDouble} " +
             s"1:${geoPointDate.substring(0, 4).toDouble/2016.0} " +
             s"2:${geoPointDate.substring(4, 6).toDouble/12.0} " +
             s"3:${geoPointDate.substring(6, 8).toDouble/31.0} " +
             s"4:${geoPointTime.substring(0, 2).toDouble/24.0} " +
             s"5:${geoPointTime.substring(2, 4).toDouble/60.0} "

           pwt.write(s"${dataNorm} \r\n")

         }
       }
       pwt.close()

       val dataFrame =  contextSQL.read.format("libsvm")
         .load("source/temp.txt")

       val splits = dataFrame.randomSplit(Array(0.9, 0.1), seed = 1234L)
       val train = splits(0)
       val test = splits(1)
       // specify layers for the neural network:
       val layers = Array[Int](5, 42, 26)

       val db  =  DBMaker
         .fileDB("weightfile.db")
         .fileLockDisable
         .closeOnJvmShutdown
         .make

       val map = db
         .hashMap("map", Serializer.STRING, Serializer.BYTE_ARRAY)
         .createOrOpen

       val loadedweight = if(map.containsKey(answerpoint.stationName)){
         SerializationUtils.deserialize(map.get(answerpoint.stationName)).asInstanceOf[org.apache.spark.ml.linalg.Vector]
       } else {
         org.apache.spark.ml.linalg.Vectors.zeros(5*42*26)
       }

       val trainer = if(loadedweight.numNonzeros>0){
         new MultilayerPerceptronClassifier()
           .setLayers(layers)
           .setBlockSize(64)
           .setSeed(1234L)
           .setMaxIter(1)
           .setInitialWeights(loadedweight)
       } else
       {
         new MultilayerPerceptronClassifier()
           .setLayers(layers)
           .setBlockSize(64)
           .setSeed(1234L)
           .setMaxIter(300)
       }

       val model = trainer.fit(train)

       val w_ = SerializationUtils.serialize(model.weights)
       map.put(answerpoint.stationName, w_)

       db.commit()

       db.close()

       val result = model.transform(test)

       result.show(15)

       val predictionAndLabels = result.select("prediction", "label")
       val evaluator = new MulticlassClassificationEvaluator()
         .setMetricName("accuracy")
       println("Accuracy:" + evaluator.evaluate(predictionAndLabels))

       val featureReq = Seq((1.0,
         Vectors.dense( answerpoint.datetime.substring(0, 4).toDouble/2016.0,
                        answerpoint.datetime.substring(5, 7).toDouble/12.0,
                        answerpoint.datetime.substring(8, 10).toDouble/31.0,
                        answerpoint.datetime.substring(11, 13).toDouble/24.0,
                        answerpoint.datetime.substring(14, 16).toDouble/60.0)))

       val requestData = contextSQL.createDataFrame(featureReq).toDF("label", "features")

       val prediction = model.transform(requestData)

       val predictedTemperature = prediction.select("prediction").head()
       if(predictedTemperature.size > 0)
         answerpoint.temperature = (predictedTemperature.getDouble(0).toInt - 13)*4

       println("Temperature forecast: ", answerpoint.temperature, " C")
     }
     val obj: JObject =
       ( "parameters" -> answerResultList.results.map {w =>
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

    Map("result" -> obj)
  }
}