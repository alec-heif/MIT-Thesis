package org.me.PrivateSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark.api.{SAR_RDD, Lap_RDD, PrivateSparkContext, Range}

object DemoSparkJob extends Serializable {

  def main(args: Array[String]): Unit = {
    Laplace.setEnabled(false)
    for (i <- 1 to 3) {
      covariance_matrix_slow()
      covariance_matrix_middle()
      covariance_matrix_fast()
    }
  }

  def covariance_matrix_slow() : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Covariance Matrix Slow"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/result_all.csv")

    val split_rdd = rdd.map(x => x.split(","))
    split_rdd.cache()

    val movie_ids = 1 to 10

    for (i <- movie_ids) {
      val i_rdd = split_rdd.filter(x => x(0).toInt == i).map(x => x(2).toDouble).setRange(new Range(0, 5))
      val i_sum = i_rdd.sum()
      val i_count = i_rdd.count()
      println(i + ", " + i_count + ", " + i_sum / i_count)
    }

    sc.stop()

  }

  def covariance_matrix_middle() : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Covariance Matrix Middle"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/result_all.csv")

    val split_rdd = rdd.map(x => x.split(",")).groupBy(x => x(0).toInt).mapValues(x => x(2))
    split_rdd.cache()

    val movie_ids = 1 to 10

    for (i <- movie_ids) {
      val i_rdd = split_rdd.setKeys(i to i).setRangeForKeys(i to i, new Range(0, 5))
      val sums = i_rdd.kSum()
      val sum : Double = sums(0)._2

      val counts = i_rdd.kCount()
      val count = counts(0)._2

      println(i + ", " + count + ", " + sum / count)
    }

    sc.stop()

  }

  def covariance_matrix_fast() : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Covariance Matrix Fast"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/result_all.csv")

    val split_rdd = rdd.map(x => x.split(",")).groupBy(x => x(0).toInt).mapValues(x => x(2))

    val movie_ids = 1 to 10

    val grouped = split_rdd.setKeys(movie_ids).setRangeForKeys(movie_ids, new Range(0, 5))
    val sums = grouped.kSum().groupBy(_._1).mapValues(x => x(0)._2)
    val counts = grouped.kCount().groupBy(_._1).mapValues(x => x(0)._2)

    for (i <- movie_ids) {
      println(i + ", " + counts(i) + ", " + sums(i) / counts(i))
    }

    sc.stop()

  }

  def netflix_average_actual() : Unit = {
      val query_name = "Netflix Average Actual"
      val sc = new SparkContext(new SparkConf().setAppName(query_name))
      val rdd = sc.textFile("hdfs:///datasets/netflix/result_all.csv")
      val transformed_rdd = rdd.map(x => x.split(",")).filter(x => x(0).equals("2")).map(x => x(2).toDouble)
      transformed_rdd.cache()

      val avg_rating = transformed_rdd.mean()
      val num_ratings = transformed_rdd.count()
      println("act_count, " + num_ratings)
      println("act_avg, " + avg_rating)

      sc.stop()
  }

  def netflix_average_private() : Unit = {
      val query_name = "Netflix Average Private"
      val sc = new PrivateSparkContext(query_name)
      val rdd = sc.getLapRDD("hdfs:///datasets/netflix/result_all.csv")
      val transformed_rdd = rdd.map(x => x.split(",")).filter(x => x(0).equals("2")).map(x => x(2).toDouble).setRange(new Range(0, 5))
      transformed_rdd.cache()

      for(i <- 1 to 10000) {
          val num_ratings = transformed_rdd.count()
          val avg_ratings = transformed_rdd.avg()
          println("priv_count, " + num_ratings)
          println("priv_avg, " + avg_ratings)
      }

      sc.stop()
  }

  def aol_compare_actual() : Unit = {
    // Names of words that you want to compare
    val first_word = "mac"
    val second_word = "pc"

    // val query_name = "aol_spark_optimized_" + exp_num
    val query_name = "AOL Comparison Actual"
    val sc = new SparkContext(new SparkConf().setAppName(query_name))

    val rdd = sc.textFile("hdfs:///datasets/aol/aol_dataset.csv")

    // Select query
    val queries = rdd.map(x => x.split("\t")(1))

    // Remove irrelevant queries
    val filtered_queries = queries.filter(x => x.contains(first_word) || x.contains(second_word))

    // Get unique queries and split into words
    val words = filtered_queries.distinct().flatMap(x => x.split(" "))

    // Find word counts
    val first_count = words.filter(x => x.equals(first_word)).count()
    val second_count = words.filter(x => x.equals(second_word)).count()

    println(query_name + ": " + first_word + "=" + first_count + ", " + second_word + "=" + second_count)
    sc.stop()
  }

  def aol_compare_private() : Unit = {
      // Names of words that you want to compare
      val first_word = "mac"
      val second_word = "pc"

      val query_name = "AOL Comparison Private"
      val sc = new PrivateSparkContext(query_name)

      val rdd = sc.getLapRDD("hdfs:///datasets/aol/aol_dataset.csv")

      // Select query
      val queries = rdd.map(x => x.split("\t")(1))

      // Remove irrelevant queries
      val filtered_queries = queries.filter(x => x.contains(first_word) || x.contains(second_word))

      // Get unique queries
      val unique_searches = filtered_queries.distinct()

      // Split into words
      // We choose 30 as maximum number of outputs per input
      val unique_words = unique_searches
      .groupByMulti(x => x.split(" ").map(y => (y, 1)), 30)
      .setKeys(List(first_word, second_word))

      // Find word counts
      val first_count = unique_words.get(first_word).count()
      val second_count = unique_words.get(second_word).count()

      println(query_name + ": " + first_word + "=" + first_count + ", " + second_word + "=" + second_count)
      sc.stop()
  }

  /**************************** End Demo ***********************/











  def aol_spark_unoptimized(exp_num : Int) : Unit = {
      // Names of words that you want to compare
      val first_word = "mac"
          val second_word = "pc"

          val query_name = "aol_spark_unoptimized_" + exp_num
          val sc = new SparkContext(new SparkConf().setAppName(query_name))

          val rdd = sc.textFile("hdfs:///datasets/aol/aol_dataset.csv")

          // Select only the second column (the query)
          val queries = rdd.map(x => x.split("\t")(1))

          // Get unique queries and split into space-separated words
          val words = queries.distinct().flatMap(x => x.split(" "))

          // Find word counts
          val first_count = words.filter(x => x.equals(first_word)).count()
          val second_count = words.filter(x => x.equals(second_word)).count()

          println(query_name + ": " + first_word + "=" + first_count + ", " + second_word + "=" + second_count)
          sc.stop()
  }

  def aol_sparklap_unoptimized(exp_num : Int) : Unit = {
    // Names of words that you want to compare
    val first_word = "mac"
    val second_word = "pc"

    val query_name = "aol_sparklap_unoptimized_" + exp_num
    val sc = new PrivateSparkContext(query_name)

    val rdd = sc.getLapRDD("hdfs:///datasets/aol/aol_dataset.csv")

    // Select query
    val queries = rdd.map(x => x.split("\t")(1))

    // Get unique queries
    val unique_searches = queries.distinct()

    // Split into words
    // We choose 30 as maximum number of outputs per input
    val unique_words = unique_searches
      .groupByMulti(x => x.split(" ").map(y => (y, 1)), 30)
      .setKeys(List(first_word, second_word))

    // Find word counts
    val first_count = unique_words.get(first_word).count()
    val second_count = unique_words.get(second_word).count()

    println(query_name + ": " + first_word + "=" + first_count + ", " + second_word + "=" + second_count)
    sc.stop()
  }

//  def aol_line(line : String) = {
//    val fields =line.split("\t")
//    //ID:Int, query:String, date:String, hour:Int, min:Int,itemrank:Int, url:String
//    val ID=fields(0)
//    val query=fields(1)
//    val date=fields(2)
//    if (fields.length == 5) {
//      var rank = 0
//      try {
//        rank = fields(3).toInt
//      } catch {
//        case n : NumberFormatException => println(line)
//      }
//      val url=fields(4)
//      (ID,query,date,rank,url)
//    } else {
//      (ID,query,date, 0, "")
//    }
//  }

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

