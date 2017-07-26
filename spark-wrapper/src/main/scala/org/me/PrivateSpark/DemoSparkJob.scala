package org.me.PrivateSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark.api.{SAR_RDD, Lap_RDD, PrivateSparkContext, Range}

object DemoSparkJob extends Serializable {

  def main(args: Array[String]): Unit = {
    for (exp_num <- 1 to 4) {
      JobRunner.run_lap(
        exp_name="AOL_Avg",
        file_name="/aol/aol_dataset.csv",
        exp_num,
        f=run_aol
      )
      val sc = new SparkContext(new SparkConf().setAppName("AOL_AVG_ACTUAL: " + exp_num))
      val rdd = sc.textFile("hdfs:///datasets/aol/aol_dataset.csv")
      val queries = rdd.map(x => x.split("\t")(1))
      val words = queries.distinct().flatMap(x => x.split(" "))
      val mac = words.filter(x => x.equals("mac")).count()
      val pc = words.filter(x => x.equals("pc")).count()
      println("ACTUAL: mac=" + mac + ", pc=" + pc)
    }
  }

  def aol_line(line : String) = {
    val fields =line.split("\t")
    //ID:Int, query:String, date:String, hour:Int, min:Int,itemrank:Int, url:String
    val ID=fields(0)
    val query=fields(1)
    val date=fields(2)
    if (fields.length == 5) {
      var rank = 0
      try {
        rank = fields(3).toInt
      } catch {
        case n : NumberFormatException => println(line)
      }
      val url=fields(4)
      (ID,query,date,rank,url)
    } else {
      (ID,query,date, 0, "")
    }
  }

  def run_aol(rdd : Lap_RDD[String], name : String) : Unit = {
    val lines = rdd.map(aol_line)
    val unique_searches = lines.map(_._1).distinct()
    val unique_words = unique_searches.groupByMulti(x => x.split(" ").map(y => (y, 1)), 10)
    val mac = unique_words.get("mac").count()
    val pc = unique_words.get("pc").count()
    println(name + ": mac=" + mac + ", pc=" + pc)
  }

//  def run_netflix(rdd : Lap_RDD[String]) : Unit = {
//    println("Total Ratings: " + rdd.count().toLong + "\n")
//    val split = split_rdd(rdd)
//    def ratings = split.map(x => x(2).toDouble).setRange(new Range(0, 5))
//    println("Average Ratings" + ratings.avg() + "\n")
//  }
//
//  def average_netflix_rating_lap(small : Boolean) : Unit = {
//    for (exp_num <- 1 to 4; hdfs_num <- 1 to 2; private_num <- 1 to 2) {
//      val hdfs = hdfs_num == 1
//      val priv = private_num == 1
//      val file_name = "/aol/aol_dataset.csv"
//
//      val sc = new PrivateSparkContext(get_name("AOL_WORD", file_name, hdfs, priv))
//      val rdd = sc.getLapRDD(get_filepath(file_name, hdfs), priv)
//
//      val ratings = rdd.map(split).map(x => x(2).toDouble).setRange(new api.Range(0, 5))
//      println()
//
//      sc.stop()
//    }
//  }
//
//  def avg_and_median_ascii_sums() : Unit = {
//    println("\nStarting SparkSAM!" + "\n")
//
//    for (query_num <- 1 to 4; query_type <- 1 to 4) {
//      val should_use_hdfs = query_type == 1 || query_type == 2
//      val should_split = query_type == 1 || query_type == 3
//
//      val file_name = "result_all.csv"
//      var file_location = ""
//      if (should_use_hdfs) {
//        file_location = "hdfs:///datasets/netflix/"
//      } else {
//        file_location = "file:///data/dig/spark/netflix/"
//      }
//      val file_path = file_location + file_name
//
//      val num_str = query_num.toString
//      val c_str = if(should_split) "s=true" else "s=false"
//      val h_str = if(should_use_hdfs) "h=true" else "h=false"
//
//      val sc = new PrivateSparkContext("ASCII: " + file_name + ", " + c_str + ", " + h_str + ", " + num_str)
//
//      val sar_rdd = sc.getSarRDD(file_path, should_split)
//      def ascii_sum = sar_rdd.map(line => {
//        line.map(_.toByte.toDouble).sum
//      })
//      val median_rating = ascii_sum.median()
//      val average_rating = ascii_sum.average()
//      println("Median ascii sum: " + median_rating + "\n")
//      println("Average ascii sum: " + average_rating + "\n")
//
//      sc.stop()
//
//    }
//
//  }
//
//  def average_netflix_rating_sam() : Unit = {
//    println("\nStarting SparkLap!" + "\n")
//
//    for (query_num <- 1 to 4; query_type <- 1 to 4) {
//      val should_use_hdfs = query_type == 1 || query_type == 2
//      val should_split = query_type == 1 || query_type == 3
//
//      val file_name = "result_all.csv"
//      var file_location = ""
//      if (should_use_hdfs) {
//        file_location = "hdfs:///datasets/netflix/"
//      } else {
//        file_location = "file:///data/dig/spark/netflix/"
//      }
//      val file_path = file_location + file_name
//
//      val num_str = query_num.toString
//      val c_str = if (should_split) "s=true" else "s=false"
//      val h_str = if (should_use_hdfs) "h=true" else "h=false"
//
//      val sc = new PrivateSparkContext(file_name + ", " + c_str + ", " + h_str + ", " + num_str)
//
//      val sar_rdd = sc.getSarRDD(file_path, should_split)
//      def split_rdd = sar_rdd.map(line => {
//        def row = line.split(',')
//        def cleaned_row = row.map(col => col.toLowerCase.trim())
//        cleaned_row
//      })
//      val ratings = split_rdd.map(x => x(2).toDouble)
//      val median_rating = ratings.median()
//      val average_rating = ratings.average()
//      println("Median rating: " + median_rating + "\n")
//      println("Average rating: " + average_rating + "\n")
//
//      sc.stop()
//
//    }
//  }
}

