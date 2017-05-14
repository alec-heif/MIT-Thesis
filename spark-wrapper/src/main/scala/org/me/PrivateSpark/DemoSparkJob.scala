package org.me.PrivateSpark

import scala.collection.mutable.ListBuffer

object Companion {
  var classCount = 0;
}
object DemoSparkJob extends Serializable {
  val buf = ListBuffer.empty[String]

  var instanceCount = 0;
  def main(args: Array[String]) {
    var bottomCount = 0
    val logFile = "data.txt" // Should be some file on your system

    val sc = new PrivateSparkContext("Simple Application")

    val rdd = sc.getLapRDD(logFile)

    var localCount = 0;

    val cleanMap = {
      (x : String) => {
        buf += x
        x
      }
    }

    var dirtyMap = (x : String) => {
      localCount += 1
      instanceCount += 1
      Companion.classCount += 1
      x
    }

    def split(s : String) : Seq[(String, (Int, Int))] = {
      def list = s.split(", ")
      def name = list(0)
      def info = (list(1).toInt, list(2).toInt)
      List((name, info))
    }

    def list = rdd.map(cleanMap)
    list.collect.iterator.foreach(println)
    println(bottomCount)
    buf.foreach(println)

    sc.stop()
  }
}

