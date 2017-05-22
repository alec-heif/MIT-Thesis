package org.me.PrivateSpark.api

import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark.{Budget, RDDCreator}

class PrivateSparkContext (name : String) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.1
  private val SCALE = 100

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(new SparkConf().setAppName(name))

  def budget = _budget

  def getLapRDD(path: String) : Lap_RDD[String] = {
    RDDCreator.create(ctx.textFile(path), budget, new Range())
  }

  def stop() : Unit = { ctx.stop() }

}
