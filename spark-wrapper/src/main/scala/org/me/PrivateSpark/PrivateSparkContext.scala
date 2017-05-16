package org.me.PrivateSpark

import org.apache.spark.{SparkConf, SparkContext}

class PrivateSparkContext (name : String) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.05
  private val SCALE = 1

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(new SparkConf().setAppName(name))

  def budget = _budget

  def getLapRDD(path: String) : SparkLapRDD[String] = {
    System.err.println("Got to creating RDD");
    if (budget.hasRemaining) {
      budget.charge(budget.epsilon)
      SparkLapRDD.create(ctx.textFile(path), budget, new Range())
    } else {
      SparkLapRDD.create(ctx.emptyRDD, budget, new Range())
    }
  }

  def stop() : Unit = { ctx.stop() }

}
