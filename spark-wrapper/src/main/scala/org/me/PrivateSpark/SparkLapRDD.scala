package org.me.PrivateSpark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.util.Random

class SparkLapRDD[T: ClassTag](
                                _delegate: RDD[T],
                                _budget: Budget,
                                _range: Range = new Range()
                                ) extends Serializable {

  private val delegate = _delegate
  def budget = _budget
  def range = _range

  def map[U: ClassTag](f: T => U, userRange: Range = range): SparkLapRDD[U] = {
    new SparkLapRDD(delegate.map(f), budget, userRange)
  }

  def filter(f: T => Boolean): SparkLapRDD[T] = {
    new SparkLapRDD(delegate.filter(f), budget, range)
  }

  def groupBy[K: ClassTag, V: ClassTag](
                            f: T => Seq[(K, V)],
                            keys: Seq[K],
                            numVals: Int,
                            userRange: Range = range)
  : SparkLapPairRDD[K, Iterable[V]] = {

    def g(input: T): Seq[(K, V)] = {

      // Returned keys must be in the input
      def keyMatch(input: (K, V)): Boolean = {
        keys.contains(input._1)
      }

      // Apply key filter and truncate to length
      def result = f(input)
        .filter(keyMatch)
        .take(numVals)

      // Also need to pad to length if necessary, so choose random k/v to pad with
      def random = new Random
      def randKey = keys(random.nextInt(keys.length))
      def randVal = result(random.nextInt(result.length))._2

      // Apply padding and return
      result.padTo(numVals, (randKey, randVal))
    }

    // TODO apply budget
    new SparkLapPairRDD(delegate.flatMap(g).groupByKey(), budget, userRange)
  }

  // TODO remove
  def collect = delegate.collect
}
