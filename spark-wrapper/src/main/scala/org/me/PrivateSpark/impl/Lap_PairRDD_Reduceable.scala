package org.me.PrivateSpark.impl

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_RDD, Lap_PairRDD, Range}

import scala.reflect.ClassTag

class Lap_PairRDD_Reduceable[K, V](
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

  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    def countedDelegate = delegate.map(x => (x._1, 1.0))
    if (budget.charge(budget.epsilon * info.outputs)) {
      countedDelegate.reduceByKey(_ + _).collect().map(Utils.noise[K](scale))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }


  def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def doubleDelegate = delegate.asInstanceOf[RDD[(K, Double)]]

    var sensitivities = Map.empty[K, Double]
    for (k <- info.ranges.keys) {
      def range = info.ranges(k)
      def sensitivity = math.max(math.abs(range.max), math.abs(range.min))
      sensitivities += (k -> sensitivity)
    }

    def scales = sensitivities.map(x => (x._1, x._2 / budget.epsilon))

    def reducibleDelegate = new PairRDDFunctions(doubleDelegate)
    def actualResult = reducibleDelegate.reduceByKey(_+ _).collect()
    if (budget.charge(budget.epsilon * info.outputs)) {
      actualResult.map(x => (x._1, x._2 + Laplace.draw(scales(x._1))))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  def kAvg()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def doubleDelegate = delegate.asInstanceOf[RDD[(K, Double)]]
    def reducibleDelegate = new PairRDDFunctions(doubleDelegate)

    def lengths = reducibleDelegate.countByKey()

    var sensitivities = Map.empty[K, Double]
    for (k <- info.ranges.keys) {
      def range = info.ranges(k)
      def sensitivity = range.width / lengths(k)
      sensitivities += (k -> sensitivity)
    }

    def scales = sensitivities.map(x => (x._1, x._2 / budget.epsilon))

    def actualResult =
      reducibleDelegate.reduceByKey(_ + _).collect().map(x => (x._1, x._2 / lengths(x._1)))

    if (budget.charge(budget.epsilon * info.outputs)) {
      actualResult.map(x => (x._1, x._2 + Laplace.draw(scales(x._1))))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }
  override def cache() : Unit = {
    delegate.cache()
  }
}

