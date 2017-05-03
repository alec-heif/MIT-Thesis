package org.me.PrivateSpark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkLapRDD[T: ClassTag](_delegate: RDD[T], _budget: Budget) extends Serializable {

  private val delegate = _delegate
  def budget = _budget

  def map[U: ClassTag](f: T => U): SparkLapRDD[U] = {
    def g = clean(f)
    new SparkLapRDD(delegate.map(g), budget)
  }

  def clean[U: ClassTag](func: T => U) : T => U = {

    // TODO need to figure out how to implement this
    func
  }

  // TODO remove
  def collect = delegate.collect
}
