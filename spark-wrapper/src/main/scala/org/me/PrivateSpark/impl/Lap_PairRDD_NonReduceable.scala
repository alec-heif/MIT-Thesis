package org.me.PrivateSpark.impl

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_RDD, Lap_PairRDD, Range}

import scala.reflect.ClassTag

class Lap_PairRDD_NonReduceable [K, V](
                                   delegate : RDD[(K, V)]
                                   , budget : Budget
                                   , info : QueryInfo[K]
                                   ) extends Lap_PairRDD[K, V] {

  override def mapValues[T](f : V => T, ranges : Map[K, Range])(implicit tag : ClassTag[T]): Lap_PairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)

    def h(input : (K, V)) : (K, T) = (input._1, g(input._2))

    RDDCreator.create(delegate.map(h), budget, info.set(ranges))
  }

  override def mapValues[T](f : V => T, range : Range)(implicit tag : ClassTag[T]): Lap_PairRDD[K, T] = {
    var ranges = Map.empty[K, Range]
    for (x <- info.keys) {
      ranges += (x -> range)
    }
    mapValues(f, ranges)
  }

  override def filterValues(f : V => Boolean)(implicit tag : ClassTag[V]) : Lap_PairRDD[K, V] = {
    val g = Cleaner.enforcePurity(f)

    def h(input : (K, V)) : Boolean = g(input._2)

    RDDCreator.create(delegate.filter(h), budget, info)

  }

  override def get(key : K)(implicit tag : ClassTag[V]) : Lap_RDD[V] = {
    def selected = delegate.filter(x => x._1.equals(key)).map(x => x._2)
    RDDCreator.create(selected, budget, info.ranges(key))
  }

  override def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def countedDelegate = delegate.map(x => (x._1, 1.0))

    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    if (budget.charge(budget.epsilon * info.outputs)) {
      countedDelegate.reduceByKey(_ + _).collect().map(Utils.noise(scale))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    throw new UnsupportedOperationException("Not permitted!")
  }

  override def kAvg()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    throw new UnsupportedOperationException("Not permitted!")
  }
  override def cache() : Unit = {
    delegate.cache()
  }
}
