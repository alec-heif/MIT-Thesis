package org.me.PrivateSpark

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

trait SparkLapRDD[T] extends Serializable {

  def map[U : ClassTag](f : T => U, range : Range = Range.default()) : SparkLapRDD[U]
  def filter(f : T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T]
  def groupBy[K, V : ClassTag] (
                                 f : T => Seq[(K, V)]
                                 , maxOutputs : Int
                                 , ranges : Map[K, Range]
                                 ) : SparkLapPairRDD[K, V]

  def count() : Double
  def max() : Double
//  def sum() : Double
//  def avg() : Double
//  def min() : Double
//  def max() : Double
//  def median() : Double

}

object SparkLapRDD {

  def create[T : ClassTag](delegate : RDD[T], budget : Budget, range : Range)(implicit tag : ClassTag[T]) : SparkLapRDD[T] = {
    val enforcer = Utils.enforce(range) _
    delegate match {
      case reducible : RDD[Double @unchecked] if tag == classTag[Double] => {
        val _delegate = delegate.asInstanceOf[RDD[Double]]
        val result = new ReducibleSparkLapRDD[Double](_delegate.map(enforcer), budget, range)
        result.asInstanceOf[SparkLapRDD[T]]
      }
      case _ => new UnreducibleSparkLapRDD[T](delegate, budget, range)
    }
  }
}

class UnreducibleSparkLapRDD[T](
                                delegate: RDD[T],
                                budget: Budget,
                                range : Range
                                ) extends SparkLapRDD[T] {

  override def map[U : ClassTag](f: T => U, range : Range = range) : SparkLapRDD[U] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.map(g), budget, range)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.filter(g), budget, range)
  }

  override def groupBy[K, V : ClassTag](
                              grouper : T => Seq[(K, V)]
                              , maxOutputs : Int
                              , ranges : Map[K, Range]
                              ) : SparkLapPairRDD[K, V] = {
    val g = Cleaner.enforcePurity(grouper)

    val info = new QueryInfo[K]().set(maxOutputs)
    val h = Utils.trim(g, info.outputs) _

    SparkLapPairRDD.create(delegate.flatMap(h), info)
  }

  override def count() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }

  override def max() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
}

// T must be a double!
class ReducibleSparkLapRDD[T](
                                 delegate: RDD[T],
                                 budget: Budget,
                                 range: Range
                                 ) extends SparkLapRDD[T] {

  override def map[U : ClassTag](f: T => U, range : Range = range) : SparkLapRDD[U] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.map(g), budget, range)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.filter(g), budget, range)
  }

  override def groupBy[K, V : ClassTag](
                                         grouper : (T) => Seq[(K, V)]
                                         , maxOutputs : Int
                                         , ranges : Map[K, Range]
                                         ) : SparkLapPairRDD[K, V] = {
    val g = Cleaner.enforcePurity(grouper)

    val info = QueryInfo.default[K]().set(maxOutputs)
    val h = Utils.trim(g, info.outputs) _

    SparkLapPairRDD.create[K, V](delegate.flatMap(h), info)
  }

  override def count() : Double = {
    delegate.count()
  }

  override def max() : Double = {
    val _delegate = delegate.asInstanceOf[RDD[Double]]
    _delegate.max()
  }
}
