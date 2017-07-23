package org.me.PrivateSpark.api

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.me.PrivateSpark.{Single_Enforcement, QueryInfo, Budget, RDDCreator}
import com.redhat.et.silex.sample.split.implicits._

import scala.collection.parallel.{ParSet, ParSeq}


class PrivateSparkContext (name : String) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.1
  private val SCALE = 100

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(new SparkConf().setAppName(name))

  def budget = _budget

  def getLapRDD(path: String) : Lap_RDD[String] = {
    RDDCreator.create(ctx.textFile(path), new QueryInfo(budget), Single_Enforcement.default())
  }

  def getSarRDD(path: String, should_coalesce : Boolean) : SAR_RDD[String] = {
    val base = ctx.textFile(path)
    val numLines = base.count()

    val numPartitions = math.round(math.pow(numLines, 0.4)).toInt
    val partitionSize = 1.0 * numLines / numPartitions
    val weights = (1 to numPartitions).map(_ => partitionSize).toArray

    var splitBase : ParSet[RDD[String]] = base.splitSample(numPartitions).toSet.par

    if (should_coalesce) {
      splitBase = splitBase.map(x => x.coalesce(1, shuffle = true))
    }

    new SAR_RDD[String](ctx, splitBase, numPartitions)

//    def real_base = Seq[RDD[String]](base)
//    new SAR_RDD(ctx, real_base, 1)
  }

  def stop() : Unit = { ctx.stop() }

}
