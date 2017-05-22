package org.me.PrivateSpark

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark.api.{Range, Lap_RDD, Lap_PairRDD}
import org.me.PrivateSpark.impl.{Lap_RDD_Reduceable, Lap_RDD_NonReduceable, Lap_PairRDD_Reduceable, Lap_PairRDD_NonReduceable}

import scala.reflect.{ClassTag, classTag}

object RDDCreator {

  def create[T : ClassTag](delegate : RDD[T], budget : Budget, range : Range)(implicit tag : ClassTag[T]) : Lap_RDD[T] = {
    delegate match {
      case reducible : RDD[Double @unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[Double]]
        val enforcer = Utils.enforce(range) _
        val result = new Lap_RDD_Reduceable[Double](_delegate.map(enforcer), budget, range)
        result.asInstanceOf[Lap_RDD[T]]
      case _ => new Lap_RDD_NonReduceable[T](delegate, budget, range)
    }
  }

  def create[K, V : ClassTag] (delegate : RDD[(K, V)], budget : Budget, info : QueryInfo[K])
                              (implicit tag : ClassTag[V]) : Lap_PairRDD[K, V] = {

    // Need to enforce every potentially reducible creation
    val matcher = Utils.keyMatch(info.keys.toSet) _

    delegate match {

      case reducible: RDD[(K, Double)@unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
        val enforcer = Utils.enforce(info.ranges) _

        val enforcedDelegate = _delegate.filter(matcher).map(enforcer)

        val result = new Lap_PairRDD_Reduceable[K, Double](enforcedDelegate, budget, info)
        result.asInstanceOf[Lap_PairRDD[K, V]]

      case _ =>
        val filteredDelegate = delegate.filter(matcher)
        new Lap_PairRDD_NonReduceable[K, V](filteredDelegate, budget, info)
    }
  }
}
