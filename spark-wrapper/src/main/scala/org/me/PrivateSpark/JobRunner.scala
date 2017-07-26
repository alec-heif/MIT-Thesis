package org.me.PrivateSpark

import org.me.PrivateSpark.api.{SAR_RDD, Lap_RDD, PrivateSparkContext}

object JobRunner {
  def get_name(prefix : String, file_name : String, use_hdfs : Boolean, is_private : Boolean, num : Int) : String = {
    val f = file_name
    val h = if (use_hdfs) "h=true" else "h=false"
    val p = if (is_private) "p=true" else "p=false"
    val n = "n=" + num
    val name = prefix + ": " + f + ", " + p + ", " + h + ", " + n
    name
  }

  def get_filepath(file_name : String, use_hdfs : Boolean) : String = {
    val file_prefix = if(use_hdfs) "hdfs:///datasets/" else "file:///data/dig/spark/"
    file_prefix + file_name
  }

  def run_sam(exp_name : String, file_name : String, exp_num : Int, f : (SAR_RDD[String], String) => Unit)
  : Unit = {
    for (hdfs_num <- 1 to 1; private_num <- 1 to 1) {
      val hdfs = hdfs_num == 1
      val priv = private_num == 1

      val name = get_name(exp_name, file_name, hdfs, priv, exp_num)

      val sc = new PrivateSparkContext(name)
      val rdd = sc.getSarRDD(get_filepath(file_name, hdfs), priv)
      f(rdd, name)
      sc.stop()
    }
  }

  def run_lap(exp_name : String, file_name : String, exp_num : Int, f : (Lap_RDD[String], String) => Unit)
  = {
    for (hdfs_num <- 1 to 1; private_num <- 1 to 1) {
      val hdfs = hdfs_num == 1
      val priv = private_num == 1

      val name = get_name(exp_name, file_name, hdfs, priv, exp_num)

      val sc = new PrivateSparkContext(name)
      val rdd = sc.getLapRDD(get_filepath(file_name, hdfs), priv)
      f(rdd, name)
      sc.stop()
    }
  }

}
