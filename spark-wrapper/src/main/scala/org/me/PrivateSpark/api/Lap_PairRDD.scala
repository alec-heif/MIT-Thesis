package org.me.PrivateSpark.api

import scala.reflect.ClassTag

trait Lap_PairRDD[K, V] extends Serializable {

  /**
   * Apply a mapping function to each value while leaving key mappings untouched.
   *
   * @param f Function to apply to all values in RDD. Must be pure.
   * @tparam T Output type of f
   * @return PairRDD with all values mapped using f
   */
  def mapValues[T : ClassTag](f : V => T) : Lap_PairRDD[K, T]

  /**
   * Apply a filtering function to all values while leaving key mappings untouched.
   *
   * @param f Function to apply to all values in RDD. Must be pure.
   * @return PairRDD containing the subset of values for which f returns true.
   */
  def filterValues(f : V => Boolean) (implicit tag : ClassTag[V]) : Lap_PairRDD[K, V]

  /**
   * Get all values for a particular key.
   *
   * @param key Key to return values from
   * @return RDD consisting of all values mapped to key
   */
  def get(key : K) (implicit tag : ClassTag[V]) : Lap_RDD[V]

  /**
   * Set the keys for this particular RDD, which will be enforced at time of reduction
   * @param keys
   * @param tag
   * @return
   */
  def setKeys(keys : Seq[K])(implicit tag : ClassTag[V]) : Lap_PairRDD[K, V]

  /**
   * Set the Ranges for this particular RDD, which will be enforced at time of reduction
   * @param ranges
   * @param tag
   * @return
   */
  def setRanges(ranges : Map[K, Range])(implicit tag : ClassTag[V]) : Lap_PairRDD[K, V]

  /**
   * Set a single Range for all keys in keys
   * @param keys
   * @param range
   * @param tag
   * @return
   */
  def setRangeForKeys(keys : Seq[K], range : Range)(implicit tag : ClassTag[V]) : Lap_PairRDD[K, V]

  /**
   * Get noisy value counts for each key. Costs privacy budget linear in the
   * number of times each input is used in the result (e.g. by multi groupings).
   *
   * @return map where each key corresponds to a noisy count of the number of values
   *         corresponding to that particular key.
   */
  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)]

  /**
   * Get noisy sums of all values for each key. Costs privacy budget linear
   * in the number of times each input is used in the result.
   * Can only be used on PairRDDs whose values are doubles.
   *
   * @return map where each key corresponds to a noisy sum of the values
   *         corresponding to that particular key.
   */
  def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)]

  /**
   * Get noisy averages of all values for each key. Costs privacy budget linear
   * in the number of times each input is used in the result.
   * Can only be used on PairRDDs whose values are doubles.
   *
   * @return map where each key corresponds to a noisy average of the values
   *         corresponding to that particular key.
   */
  def kAvg()(implicit tag : ClassTag[K]) : Seq[(K, Double)]

  /**
   * Hint to SparkLAP to cache the RDD in memory on worker nodes.
   * This can be used to speed up later computations on RDDs whose data
   * is used repeatedly.
   */
  def cache() : Unit
}








