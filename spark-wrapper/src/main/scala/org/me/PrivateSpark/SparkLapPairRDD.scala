package org.me.PrivateSpark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkLapPairRDD[K: ClassTag, V: ClassTag](
                                                 _delegate: RDD[(K, V)],
                                                 _budget: Budget,
                                                 _range: Range
                                                 ) extends Serializable {

  private val delegate = _delegate
  def budget = _budget
  def range = _range

  def mapValues[U: ClassTag](f: V => U, userRange: Range = range): SparkLapPairRDD[K, U] = {
    new SparkLapPairRDD(delegate.mapValues(f), budget, userRange)
  }

  def collect = delegate.collect
}
