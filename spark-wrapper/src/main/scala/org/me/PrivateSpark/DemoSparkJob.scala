package org.me.PrivateSpark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.me.PrivateSpark.api.{SAR_RDD, Lap_RDD, PrivateSparkContext, Range}

object DemoSparkJob extends Serializable {

  def main(args: Array[String]): Unit = {
    for(i <- 1 to 5) {
      average_netflix_rating_nonprivate(i)
    }
    for(i <- 1 to 5) {
      average_netflix_rating_cached_nonprivate(i)
    }
    for(i <- 1 to 5) {
      average_netflix_rating_giant_cached_nonprivate(i)
    }
    for(i <- 1 to 5) {
      many_outputs_nonprivate(i)
    }
  }

  def many_outputs_nonprivate(i : Int) : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Many Outputs " + i
    val sc = new SparkContext(new SparkConf().setAppName(query_name))
    val rdd = sc.textFile("hdfs:///datasets/netflix/large/result_all.csv")

    val split_rdd = rdd.map(x => x.split(","))
    val key_rdd = split_rdd.map({case line => line(0).toInt->line(2).toDouble})
    val sums = key_rdd.reduceByKey(_ + _).collectAsMap()
    val counts = key_rdd.map(x => (x._1, 1.0)).reduceByKey(_ + _).collectAsMap()

    val movie_ids = 1 to 1000

    for (id <- movie_ids) {
      println(id + ", " + sums(i) / counts(i))
    }

    sc.stop()

  }
  def average_netflix_rating_giant_cached_nonprivate(i : Int) : Unit = {
    val query_name = "Average Rating Giant Cached: " + i
    val sc = new SparkContext(new SparkConf().setAppName(query_name))
    val rdd = sc.textFile("hdfs:///datasets/netflix/giant/result_giant.csv")

    val start_time = System.currentTimeMillis()
    var last_time = start_time

    rdd.cache()

    for (j <- 1 to 10) {
      val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble)
      val avg = ratings.mean()
      val curr_time = System.currentTimeMillis()
      println("Spark Core_giant, " + i + ", " + j + ", " + ((curr_time - last_time) / 1000).toInt)
      last_time = curr_time
    }
    sc.stop()
  }

  def average_netflix_rating_cached_nonprivate(i : Int) : Unit = {
    val query_name = "Average Rating Cached: " + i
    val sc = new SparkContext(new SparkConf().setAppName(query_name))
    val rdd = sc.textFile("hdfs:///datasets/netflix/large/result_all.csv")

    val start_time = System.currentTimeMillis()
    var last_time = start_time

    rdd.cache()

    for (j <- 1 to 10) {
      val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble)
      val avg = ratings.mean()
      val curr_time = System.currentTimeMillis()
      println("Spark Core_small, " + i + ", " + j + ", " + ((curr_time - last_time) / 1000).toInt)
      last_time = curr_time
    }
    sc.stop()
  }

  def average_netflix_rating_nonprivate(i : Int) : Unit = {
    val query_name = "Average Rating: " + i

    val sc = new SparkContext(new SparkConf().setAppName(query_name))
    val rdd = sc.textFile("hdfs:///datasets/netflix/large/result_all.csv")

    val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble)
    val avg = ratings.mean()
    println("Spark Core, " + i + ", ")

    sc.stop()
  }

  def average_netflix_rating_giant_cached(i : Int) : Unit = {
    val query_name = "Average Rating Giant Cached: " + i
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/giant/result_giant.csv", true)

    val start_time = System.currentTimeMillis()
    var last_time = start_time

    rdd.cache()

    for (j <- 1 to 10) {
      val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble).setRange(new api.Range(0, 5))
      val avg = ratings.avg()
      val curr_time = System.currentTimeMillis()
      println("giant_average_rating_spark_lap, i, " + i + ", j, " + j + ", last, " + (curr_time - last_time) + ", total, " + (curr_time - start_time))
      last_time = curr_time
    }
    sc.stop()
  }

  def average_netflix_rating_giant(i : Int) : Unit = {
    val query_name = "Average Rating Giant: " + i

    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/giant/result_giant.csv", true)
  
    val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble).setRange(new api.Range(0, 5))
    println(ratings.avg())
  
    sc.stop()
  }

  def average_netflix_rating_cached(i : Int) : Unit = {
    val query_name = "Average Rating Cached: " + i
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/large/result_all.csv", true)

    val start_time = System.currentTimeMillis()
    var last_time = start_time

    rdd.cache()

    for (j <- 1 to 10) {
        val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble).setRange(new api.Range(0, 5))
        val avg = ratings.avg()
        val curr_time = System.currentTimeMillis()
        println("i, " + i + ", j, " + j + ", last, " + (curr_time - last_time) + ", total, " + (curr_time - start_time))
        last_time = curr_time
    }
    sc.stop()
  }

  def average_netflix_rating(i : Int) : Unit = {
    val query_name = "Average Rating: " + i

    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/large/result_all.csv", true)

    val ratings = rdd.map(x => x.split(",")).map(x => x(2).toDouble).setRange(new api.Range(0, 5))
    println(ratings.avg())

    sc.stop()
  }

  
  def median_netflix_rating(is_private : Boolean) : Unit = {
    val mod = if (is_private) "Private" else "Actual"
    val query_name = "Median Rating " + mod
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/netflix/large/result_all.csv", is_private)

    val hashed_rdd = rdd.map(x => {
      val line = x.split(",")
      val rating = line(2).toDouble
      rating
    })

    val median_hash = hashed_rdd.median()
    println(query_name + ": " + median_hash)

    sc.stop()
  }

  def median_netflix_hash_actual(hash_width : Long) : Unit = {
    val query_name = "Median Hash Actual " + hash_width
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/netflix/large/result_all.csv", false)

    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    })

    val median_hash = hashed_rdd.median()
    println("Median Actual " + hash_width + ": " + median_hash)

    sc.stop()
  }

  def median_netflix_hash_priv(hash_width : Long) : Unit = {
    val query_name = "Median Hash Private " + hash_width
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/netflix/large/result_all.csv", true)

    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    })

    val median_hash = hashed_rdd.median()
    println("Median Private " + hash_width + ": " + median_hash)

    sc.stop()
  }

  def long_median_hash_priv() : Unit = {
    for (i <- 1 to 1000) {
      val query_name = "Median Hash Private " + i
      val sc = new PrivateSparkContext(query_name)
      val rdd = sc.getSarRDD("hdfs:///datasets/aol/aol_dataset.csv", true)
      val hash_width = 1000000
      val hashed_rdd = rdd.map(x => {
        val result = math.abs(x.hashCode) % hash_width
        result.toDouble
      })

      val median_hash = hashed_rdd.median()
      println("median_hash_results, " + median_hash)
      sc.stop()
    }
  }

  def median_hash_priv(i : Int) : Unit = {
    val query_name = "Median Hash Private " + i
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/aol/aol_dataset.csv", true)

    val hash_width = 1000000
    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    })

    val median_hash = hashed_rdd.median()
    println("median_hash_results, " + median_hash)

    sc.stop()
  }

  def median_hash_actual() : Unit = {
    val query_name = "Median Hash Actual"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/aol/aol_dataset.csv", false)

    val hash_width = 1000000
    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    })

    val median_hash = hashed_rdd.median()
    println("Median Actual: " + median_hash)

    sc.stop()
  }

  def average_hash_sam() : Unit = {
    val query_name = "Average Hash SparkSAM"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getSarRDD("hdfs:///datasets/aol/aol_dataset.csv", true)

    val hash_width = 1000000
    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    })

    val average_hash = hashed_rdd.average()
    println("SAM Average: " + average_hash)

    sc.stop()
  }

  def average_hash_lap() : Unit = {
    val query_name = "Average Hash SparkLAP"
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/aol/aol_dataset.csv", true)

    val hash_width = 1000000
    val hashed_rdd = rdd.map(x => {
      val result = math.abs(x.hashCode) % hash_width
      result.toDouble
    }).setRange(new Range(0, hash_width))

    val average_hash = hashed_rdd.avg()
    println("LAP Average: " + average_hash)

    sc.stop()
  }

  def covariance_matrix_slow(range : Int) : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Covariance Matrix Slow " + range
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/large/result_all.csv")

    val split_rdd = rdd.map(x => x.split(",")).map(x => (x(0).toInt, x(2).toDouble))
    split_rdd.cache()

    val movie_ids = 1 to range

    for (i <- movie_ids) {
      val i_rdd = split_rdd.filter(x => x._1 == i).map(x => x._2).setRange(new Range(0, 5))
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

    val split_rdd = rdd.map(x => x.split(",")).groupBy(x => x(0).toInt).mapValues(x => x(2).toDouble)
    val movie_ids = 1 to 100

    val i_rdd = split_rdd.setKeys(movie_ids).setRangeForKeys(movie_ids, new Range(0, 5))
    i_rdd.cache()

    for (i <- movie_ids) {
      val sum = i_rdd.get(i).sum()
      val count = i_rdd.get(i).count()

      println(i + ", " + count + ", " + sum / count)
    }

    sc.stop()

  }

  def covariance_matrix_fast(range : Int) : Unit = {
    // Movie_ID, User_ID, Rating, YYYY-MM-DD
    val query_name = "Covariance Matrix Fast " + range
    val sc = new PrivateSparkContext(query_name)
    val rdd = sc.getLapRDD("hdfs:///datasets/netflix/large/result_all.csv")

    val split_rdd = rdd.map(x => x.split(",")).groupBy(x => x(0).toInt).mapValues(x => x(2).toDouble)

    val movie_ids = 1 to range

    val grouped = split_rdd.setKeys(movie_ids).setRangeForKeys(movie_ids, new Range(0, 5))
    val avgs = grouped.kAvg()

    avgs.foreach(x => println(x._1 + ", avg, " + x._2))

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

