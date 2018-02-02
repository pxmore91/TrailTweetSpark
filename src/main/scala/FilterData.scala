import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object FilterData {
  def main(args: Array[String]) {
    val t1 = System.nanoTime
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("TrailTweets")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("src/main/resources/hike_001.csv")
    val result = df.filter("followers_count <= 1000")

    //val result = df.where($"screen_name".isin("pctnews", "PCTAssociation", "leavenotrace", "CDNST1", "AT_Conservancy"))
    //val result = df.where($"text".like("%Appalachian%"))
    result.show()
    println("Number of records " + result.count())
    //  save results to csv
//    result.write.format("com.databricks.spark.csv")
//      .option("header", "true") // Use first line of all files as header
//      .option("inferSchema", "true").csv("/tmp/conservancy")

    // print result
    //result.collect.foreach(println)
    val duration = (System.nanoTime - t1) / 1e9d
    println("execution time:" + duration)
  }
}
