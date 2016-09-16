import java.util.{Date}

import io.hydrosphere.mist.MistJob
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable.ListBuffer

import org.json4s._
import org.json4s.JsonDSL._


object LocalWeatherRandom extends MistJob {

  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {

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
      val timeInPoint = new DateTime (now).withZone (DateTimeZone.UTC).plusSeconds((durationValue / points.length * idx ).toInt)
      val result = new Result (currentPoint, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextFloat * 100).toFloat, (r.nextInt(30)).toInt, timeInPoint.toString () )
      resList += result
    }

    val answer = new ResultList(resList.toList)

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

    Map("result" -> obj)
  }
}