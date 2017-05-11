package org.me.PrivateSpark

object Fart {
  var value = 0
}

object DemoSparkJob {
  def main(args: Array[String]) {
    var bottomCount = 0
    val logFile = "data.txt" // Should be some file on your system

    val sc = new PrivateSparkContext("Simple Application")

    val rdd = sc.getLapRDD(logFile)

    def pureMap(x : Any) = {
      Fart.value = 5
      increment()
      x
    }

    def increment() = {
      bottomCount += 1
    }

    def split(s : String) : Seq[(String, (Int, Int))] = {
      def list = s.split(", ")
      def name = list(0)
      def info = (list(1).toInt, list(2).toInt)
      List((name, info))
    }

    def list = rdd.map(pureMap)
    list.collect.iterator.foreach(println)
    println(bottomCount)
    println(Fart.value)

    sc.stop()
  }
}

