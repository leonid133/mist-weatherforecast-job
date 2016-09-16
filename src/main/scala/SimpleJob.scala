import io.hydrosphere.mist.MistJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by lblokhin on 08.04.16.
  */
object SimpleJob extends MistJob {
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
    val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
    val rdd = context.parallelize(numbers)
    Map("result" -> rdd.map(x => x * 2).collect())
  }
}

object SimpleContext extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
    val numbers: List[BigInt] = parameters("digits").asInstanceOf[List[BigInt]]
    val rdd = context.parallelize(numbers)
    Map("result" -> rdd.map(x => x * 2).collect())
  }
}

object SimpleHiveContext extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param context    spark hive context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = {

    val df = context.read.json(parameters("file").asInstanceOf[String])
    df.printSchema()
    df.registerTempTable("people")

    Map("result" -> context.sql("SELECT AVG(age) AS avg_age FROM people").collect())

  }
}

object SimpleSQLContext extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param context    spark sql context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = {
    val df = context.read.json(parameters("file").asInstanceOf[String])
    df.registerTempTable("people")
    Map("result" -> context.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}

object TestError extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result exception Test Error
    */
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
    throw new Exception("Test Error")
  }
}

object noDoStuffMethodError extends MistJob {

}
