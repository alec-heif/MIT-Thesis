package org.me.PrivateSpark.api

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark._
import com.redhat.et.silex.sample.split.implicits._

import scala.collection.parallel.{ParSet, ParSeq}


class PrivateSparkContext (name : String) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.1
  private val SCALE = 100000

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(new SparkConf().setAppName(name))

  def budget = _budget

  def getLapRDD(path: String, is_private : Boolean = true) : Lap_RDD[String] = {
    val lap_enabled = if (is_private) true else false
    Laplace.setEnabled(lap_enabled)
    RDDCreator.create(ctx.textFile(path), new QueryInfo(budget), Single_Enforcement.default())
  }

  def getSarRDD(path: String, is_private : Boolean = true) : SAR_RDD[String] = {
    val base = ctx.textFile(path).cache()
    if (is_private) {
      Laplace.setEnabled(true)
      val numLines = base.count()

      val numPartitions = math.round(math.pow(numLines, 0.4)).toInt

      var splitBase : ParSet[RDD[String]] = base.splitSample(numPartitions).toSet.par

      splitBase = splitBase.map(x => x.coalesce(1, shuffle = true))

      new SAR_RDD[String](ctx, splitBase, numPartitions)
    }
    else {
      Laplace.setEnabled(false)
      def real_base = Seq[RDD[String]](base).toSet.par
      new SAR_RDD(ctx, real_base, 1)
    }

  }

  def stop() : Unit = { ctx.stop() }

}
