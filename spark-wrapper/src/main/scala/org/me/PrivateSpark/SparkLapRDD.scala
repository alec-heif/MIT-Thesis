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

    // TODO apply budget
    def g(input: T): Seq[(K, V)] = {

      // Returned keys must be in the input
      def keyMatch(input: (K, V)): Boolean = {
        keys.contains(input._1)
      }

      // Length of returned lists must match given length
      def random = new Random
      def randKey = keys(random.nextInt(keys.length))
      def randVal = classOf[V].getConstructors.head.newInstance().asInstanceOf[V]

      // Apply rules
      f(input)
        .filter(keyMatch)
        .padTo(numVals, (randKey, randVal))
        .take(numVals)
    }
    new SparkLapPairRDD(delegate.flatMap(g).groupByKey(), budget, userRange)
  }

  // TODO remove
  def collect = delegate.collect
}
