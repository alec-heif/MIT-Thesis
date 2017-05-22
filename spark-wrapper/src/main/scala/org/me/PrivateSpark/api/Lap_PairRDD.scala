package org.me.PrivateSpark.api

import scala.reflect.ClassTag

trait Lap_PairRDD[K, V] extends Serializable {

  def mapValues[T : ClassTag](f : V => T, ranges : Map[K, Range]) : Lap_PairRDD[K, T]

  // Shortcut for if all keys have the same range
  def mapValues[T : ClassTag](f : V => T, range : Range) : Lap_PairRDD[K, T]

  def filterValues(f : V => Boolean) (implicit tag : ClassTag[V]) : Lap_PairRDD[K, V]
  def get(key : K) (implicit tag : ClassTag[V]) : Lap_RDD[V]

  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)]
  def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)]
  def kAvg()(implicit tag : ClassTag[K]) : Seq[(K, Double)]

  def cache() : Unit
}
