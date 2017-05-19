package org.me.PrivateSpark

import org.apache.spark.{SparkConf, SparkContext}

class PrivateSparkContext (name : String) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.05
  private val SCALE = 10

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(new SparkConf().setAppName(name))

  def budget = _budget

  def getLapRDD(path: String) : SparkLapRDD[String] = {
    SparkLapRDD.create(ctx.textFile(path), budget, new Range())
  }

  def stop() : Unit = { ctx.stop() }

}
