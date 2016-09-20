case class Result(point: Map[String, String], cloud: Float, sun: Float, rain: Float, var temperature: Int, datetime: String, var stationName: String)
case class ResultList(results: List[Result])
case class Duration(value: Int, text: String)
case class Distance(value: Int, text: String)
case class Legs(duration: Duration, distance: Distance)