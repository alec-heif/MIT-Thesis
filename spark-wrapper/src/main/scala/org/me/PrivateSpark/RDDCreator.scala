package org.me.PrivateSpark

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark.api.{Range, Lap_RDD, Lap_PairRDD}
import org.me.PrivateSpark.impl.{Lap_RDD_Reduceable, Lap_RDD_NonReduceable, Lap_PairRDD_Reduceable, Lap_PairRDD_NonReduceable}

import scala.reflect.{ClassTag, classTag}

object RDDCreator {

  def create[T : ClassTag](delegate : RDD[T], info : QueryInfo, enforcement : Single_Enforcement)
                          (implicit tag : ClassTag[T]) : Lap_RDD[T] = {
    delegate match {
      case reducible : RDD[Double @unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[Double]]
        val result = new Lap_RDD_Reduceable[Double](_delegate, info, enforcement)
        result.asInstanceOf[Lap_RDD[T]]
      case _ =>
        new Lap_RDD_NonReduceable[T](delegate, info, enforcement)
    }
  }

  def create[K, V : ClassTag] (delegate : RDD[(K, V)], info : QueryInfo,
                               enforcement: Pair_Enforcement[K])
                              (implicit tag : ClassTag[V]) : Lap_PairRDD[K, V] = {
    delegate match {
      case reducible: RDD[(K, Double)@unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
        val result = new Lap_PairRDD_Reduceable[K, Double](_delegate, info, enforcement)
        result.asInstanceOf[Lap_PairRDD[K, V]]
      case _ =>
        new Lap_PairRDD_NonReduceable[K, V](delegate, info, enforcement)
    }
  }

  def enforceRanges[K, V : ClassTag](delegate : RDD[(K, V)], info : QueryInfo,
                                     enforcement : Pair_Enforcement[K]) (implicit tag : ClassTag[V])
  : Lap_PairRDD_Reduceable[K, Double] = {
    val _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
    val enforcer = Utils.enforce(enforcement.ranges) _
    new Lap_PairRDD_Reduceable[K, Double](_delegate.map(enforcer), info, enforcement)
  }

  // Key enforcement needs to happen for both Reduceable and NonReduceable RDDs!
  def filterKeys[K, V : ClassTag](delegate : RDD[(K, V)], info : QueryInfo,
                                   enforcement : Pair_Enforcement[K])
  : Lap_PairRDD[K, V] = {
    val matcher = Utils.keyMatch(enforcement.keys.toSet) _
    create(delegate.filter(matcher), info, enforcement)
  }
}
