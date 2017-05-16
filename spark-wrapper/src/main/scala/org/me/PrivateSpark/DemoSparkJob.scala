package org.me.PrivateSpark

object DemoSparkJob extends Serializable {

  def main(args: Array[String]) {
    val logFile = "data.txt" // Should be some file on your system

    val sc = new PrivateSparkContext("Simple Application")

    val rdd = sc.getLapRDD(logFile)

    val basic = rdd.map(x => x.split(", ")(1).toDouble)

    println(basic.max())

    sc.stop()
  }
}

