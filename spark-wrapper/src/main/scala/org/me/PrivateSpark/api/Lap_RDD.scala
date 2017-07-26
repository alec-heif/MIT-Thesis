package org.me.PrivateSpark.api

import scala.reflect.ClassTag

trait Lap_RDD[T] extends Serializable {

  /**
   * Hint to SparkLAP to cache the RDD in memory on worker nodes.
   * This can be used to speed up later computations on RDDs whose data
   * is used repeatedly.
   */
  def cache() : Unit

  /**
   * Returns an RDD of only unique elements with this RDD.
   * @return
   */
  def distinct()(implicit tag : ClassTag[T]) : Lap_RDD[T]

  /**
   * Takes in a function and applies it to each element in the RDD, returning a new
   * RDD consisting of the results of the function after being applied to each element.
   *
   * @param f Pure function applied to each element of the RDD.
   * @tparam U Output type of f.
   * @return RDD consisting of all and only the results of f with length unchanged.
   */
  def map[U : ClassTag] (
                          f : T => U
                          ) : Lap_RDD[U]

  /**
   * Takes in a function and applies it to each element in the RDD, returning a new
   * RDD consisting of all and only elements of the RDD for which the function returns true.
   *
   * @param f Pure function applied to each element of the RDD.
   * @return non-pair RDD consisting of all and only the elements in the original RDD for which f
   *         returns true.
   */
  def filter(
              f : T => Boolean
              ) (implicit tag : ClassTag[T]) : Lap_RDD[T]

  /**
   * Convert RDD into a Pair_RDD consisting of keys and a list of values that map to that key.
   * Requires that keys be specified by the user to avoid leaking information through keys.
   *
   * @param f Function mapping each value to exactly one key.
   * @tparam K Output type of f.
   * @return Pair_RDD with keys in K mapped to values in the source RDD.
   */
    def groupBy[K] ( f : T => K ) (implicit tag : ClassTag[T]) : Lap_PairRDD[K, T]

  /**
   * Convert RDD into a Pair_RDD consisting of keys and a list of values that map to that key.
   *
   * @param f
   * @param maxOutputs
   * @tparam K
   * @tparam V
   * @return
   */
  def groupByMulti[K, V : ClassTag] (
                                 f : T => Seq[(K, V)]
                                 , maxOutputs : Int = 1
                                 ) : Lap_PairRDD[K, V]


  /**
   * Set the Range for this particular RDD, which will be enforced at time of reduction
   * @param range
   * @param tag
   * @return
   */
  def setRange(range : Range)(implicit tag : ClassTag[T]) : Lap_RDD[T]


  def count() : Double
  def sum() : Double
  def avg() : Double
}
