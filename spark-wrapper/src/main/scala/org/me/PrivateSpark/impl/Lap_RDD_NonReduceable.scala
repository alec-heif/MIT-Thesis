package org.me.PrivateSpark.impl

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_PairRDD, Lap_RDD}

import scala.reflect.ClassTag

class Lap_RDD_NonReduceable[T](
                                 delegate: RDD[T],
                                 budget: Budget,
                                 range : api.Range = new api.Range()
                                 ) extends Lap_RDD[T] {

  override def cache() : Unit = {
    delegate.cache()
  }

  override def map[U : ClassTag](f: T => U, _range : api.Range = range) : Lap_RDD[U] = {
    val g = Cleaner.enforcePurity(f)
    RDDCreator.create(delegate.map(g), budget, _range)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): Lap_RDD[T] = {
    val g = Cleaner.enforcePurity(f)
    RDDCreator.create(delegate.filter(g), budget, range)
  }

  override def groupBy[K] ( f : T => K , keys : Seq[K] )
                          (implicit tag : ClassTag[T]) : Lap_PairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)
    def multiGroup(input : T) : Seq[(K, T)] = Seq((g(input), input))

    // All keys have the same range
    var ranges = Map.empty[K, api.Range]
    for (key <- keys) {
      ranges = ranges + (key -> range)
    }

    groupByMulti(multiGroup, keys, 1, ranges)
  }

  override def groupByMulti[K, V : ClassTag](
                                         grouper : T => Seq[(K, V)]
                                         , keys : Seq[K]
                                         , maxOutputs : Int
                                         , ranges : Map[K, api.Range] = Map.empty[K, api.Range]
                                         ) : Lap_PairRDD[K, V] = {
    val g = Cleaner.enforcePurity(grouper)

    val info = new QueryInfo[K](keys).set(maxOutputs).set(ranges)
    val h = Utils.trim(g, info.outputs) _

    RDDCreator.create(delegate.flatMap(h), budget, info)
  }

  override def count() : Double = {
    // Special case: count is allowed on all data types
    if (budget.charge(budget.epsilon)) {
      def sensitivity = 1.0
      Utils.noisify(delegate.count(), sensitivity, budget)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def sum() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
  override def avg() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
}

