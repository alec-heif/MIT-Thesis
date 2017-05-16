package org.me.PrivateSpark

object DemoSparkJob extends Serializable {

  def main(args: Array[String]) {
    val logFile = "data.txt" // Should be some file on your system

    val sc = new PrivateSparkContext("Simple Application")

    val rdd = sc.getLapRDD(logFile)

    def grouper(input : String) : Seq[(String, Seq[String])] = {
      def split = input.split(", ")
      Seq((split(1), split.toSeq))
    }

    def getPrice(input : Seq[String]) : Double = {
      input(3).toDouble
    }

    val groupedRdd = rdd.groupBy(grouper, 1, Map.empty[String, Range])
    var ranges = Map.empty[String, Range]
    ranges += ("foo" -> new Range(0, 5))
    ranges += ("bar" -> new Range(0, 10))
    val doubledRdd = groupedRdd.mapValues(getPrice, ranges)

    for (i <- 1 to 20) {
      doubledRdd.kCount().foreach(println)
    }

    sc.stop()
  }
}

