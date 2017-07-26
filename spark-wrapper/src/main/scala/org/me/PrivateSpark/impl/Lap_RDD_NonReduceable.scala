package org.me.PrivateSpark.impl

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_PairRDD, Lap_RDD}

import scala.reflect.ClassTag

class Lap_RDD_NonReduceable[T](
                                 delegate: RDD[T]
                                 , info: QueryInfo
                                 , enforcement : Single_Enforcement
                                 ) extends Lap_RDD[T] {

  override def cache() : Unit = {
    delegate.cache()
  }

  override def distinct()(implicit tag : ClassTag[T]): Lap_RDD[T] = {
    RDDCreator.create(delegate.distinct(), info, enforcement)
  }

  override def map[U : ClassTag](f: T => U) : Lap_RDD[U] = {
    val g = Cleaner.enforcePurity(f)
    RDDCreator.create(delegate.map(g), info, enforcement)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): Lap_RDD[T] = {
    val g = Cleaner.enforcePurity(f)
    RDDCreator.create(delegate.filter(g), info, enforcement)
  }

  override def groupBy[K] ( f : T => K)
                          (implicit tag : ClassTag[T]) : Lap_PairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)
    def multiGroup(input : T) : Seq[(K, T)] = Seq((g(input), input))

    groupByMulti(multiGroup, 1)
  }

  override def groupByMulti[K, V : ClassTag](
                                         grouper : T => Seq[(K, V)]
                                         , maxOutputs : Int
                                         ) : Lap_PairRDD[K, V] = {

    // Have to ensure no more than maxOutputs per input
    val g = Cleaner.enforcePurity(grouper)
    val h = if (Laplace.getEnabled) Utils.trim(g, maxOutputs) _ else g
    val newDelegate = delegate.flatMap(h)

    // Existing range info is thrown away
    val newEnforcement = Pair_Enforcement.default[K]()

    // Outputs are multiplied by the max outputs for this query
    val newInfo = info.scaleOutputs(maxOutputs)

    // And now add all the new things
    RDDCreator.create[K, V](newDelegate, newInfo, newEnforcement)
  }

  override def setRange(range: api.Range)(implicit tag: ClassTag[T]): Lap_RDD[T] = {
    throw new UnsupportedOperationException("Only RDDs of Doubles can have Ranges enforced!")
  }

  override def count() : Double = {
    // Special case: count is allowed on all data types
    def budget = info.budget
    if (budget.charge(info.outputs * budget.epsilon)) {
      def sensitivity = 1.0
      def scale = sensitivity / budget.epsilon
      delegate.count() + Laplace.draw(scale)
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

