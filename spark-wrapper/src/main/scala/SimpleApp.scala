/* SimpleApp.scala */
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "data.txt" // Should be some file on your system

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new PrivateSparkContext(conf)

    val logData = sc.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")

    val failedLogData = sc.textFile(logFile).cache()
    val failedNumAs = failedLogData.filter(line => line.contains("a")).count()
    val failedNumBs = failedLogData.filter(line => line.contains("b")).count()
    println(s"Failed Lines with a: $failedNumAs, Failed Lines with b: $failedNumBs")
    sc.stop()
  }
}

