package org.me.PrivateSpark

object DemoSparkJob {
  var x = ""

  def main(args: Array[String]) {
    val logFile = "data.txt" // Should be some file on your system

    val sc = new PrivateSparkContext("Simple Application")

    val rdd = sc.getLapRDD(logFile)

    def mapFn(s : String) : String = {
//      save(s)
      if(s.equals("aaaa")) {
        return "first"
      } else {
        return s
      }
    }

    rdd.map(mapFn).collect.iterator.foreach(println)
    println(x)

    sc.stop()
  }

  def save(y : String): Unit = {
    x = y
  }
}

