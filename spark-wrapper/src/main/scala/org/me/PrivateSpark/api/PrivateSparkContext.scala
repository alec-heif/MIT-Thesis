package org.me.PrivateSpark.api

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark.{Budget, RDDCreator}
import com.redhat.et.silex.sample.split.implicits._

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

  def getSarRDD(path: String) : SAR_RDD[String] = {
    def base = ctx.textFile(path)
    def numLines = base.count()

//     def numPartitions = math.round(math.pow(numLines, 0.4)).toInt
//     def splitBase = base.splitSample(numPartitions)
//     new SAR_RDD(ctx, splitBase, numPartitions)

    def real_base = Seq[RDD[String]](base)
    new SAR_RDD(ctx, real_base, 1)
  }

  def stop() : Unit = { ctx.stop() }

}
