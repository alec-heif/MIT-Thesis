package org.me.PrivateSpark

import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, classTag}

trait SparkLapRDD[T] extends Serializable {

  def cache() : Unit

  def map[U : ClassTag](f : T => U, range : Range = Range.default()) : SparkLapRDD[U]
  def filter(f : T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T]
  def groupBy[K, V : ClassTag] (
                                 f : T => Seq[(K, V)]
                                 , keys : Seq[K]
                                 , maxOutputs : Int = 1
                                 , ranges : Map[K, Range] = Map.empty[K, Range]
                                 ) : SparkLapPairRDD[K, V]

  def count() : Double
  def sum() : Double
  def avg() : Double
  def min() : Double
  def max() : Double
  def median() : Double
}

object SparkLapRDD {

  def create[T : ClassTag](delegate : RDD[T], budget : Budget, range : Range)(implicit tag : ClassTag[T]) : SparkLapRDD[T] = {
    delegate match {
      case reducible : RDD[Double @unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[Double]]
        val enforcer = Utils.enforce(range) _
        val result = new ReducibleSparkLapRDD[Double](_delegate.map(enforcer), budget, range)
        result.asInstanceOf[SparkLapRDD[T]]
      case _ => new UnreducibleSparkLapRDD[T](delegate, budget, range)
    }
  }
}

class UnreducibleSparkLapRDD[T](
                                delegate: RDD[T],
                                budget: Budget,
                                range : Range
                                ) extends SparkLapRDD[T] {

  override def cache() : Unit = {
    delegate.cache()
  }

  override def map[U : ClassTag](f: T => U, _range : Range = range) : SparkLapRDD[U] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.map(g), budget, _range)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.filter(g), budget, range)
  }

  override def groupBy[K, V : ClassTag](
                              grouper : T => Seq[(K, V)]
                              , keys : Seq[K]
                              , maxOutputs : Int = 1
                              , ranges : Map[K, Range] = Map.empty[K, Range]
                              ) : SparkLapPairRDD[K, V] = {
    val g = Cleaner.enforcePurity(grouper)

    val info = new QueryInfo[K](keys).set(maxOutputs)
    val h = Utils.trim(g, info.outputs) _

    SparkLapPairRDD.create(delegate.flatMap(h), budget, info)
  }

  override def count() : Double = {
    // Special case: count is allowed on all data types
    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    if (budget.charge(budget.epsilon)) {
      delegate.count() + SparkLaplace.draw(scale)
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
  override def max() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
  override def min() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
  override def median() : Double = {
    throw new UnsupportedOperationException("Not permitted!")
  }
}

// T must be a double!
class ReducibleSparkLapRDD[T](
                                 delegate: RDD[T],
                                 budget: Budget,
                                 range: Range
                                 ) extends SparkLapRDD[T] {

  override def cache() : Unit = {
    delegate.cache()
  }

  override def map[U : ClassTag](f: T => U, _range : Range = range) : SparkLapRDD[U] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.map(g), budget, _range)
  }

  override def filter(f: T => Boolean)(implicit tag : ClassTag[T]): SparkLapRDD[T] = {
    val g = Cleaner.enforcePurity(f)
    SparkLapRDD.create(delegate.filter(g), budget, range)
  }

  override def groupBy[K, V : ClassTag](
                                         grouper : (T) => Seq[(K, V)]
                                         , keys : Seq[K]
                                         , maxOutputs : Int = 1
                                         , ranges : Map[K, Range] = Map.empty[K, Range]
                                         ) : SparkLapPairRDD[K, V] = {
    val g = Cleaner.enforcePurity(grouper)

    val info = QueryInfo.default[K](keys).set(maxOutputs)
    val h = Utils.trim(g, info.outputs) _

    SparkLapPairRDD.create[K, V](delegate.flatMap(h), budget, info)
  }

  override def count() : Double = {
    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    if (budget.charge(budget.epsilon)) {
      delegate.count() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  // Shortcut to avoid casting every time
  def doubleDelegate = delegate.asInstanceOf[RDD[Double]]

  override def sum() : Double = {
    def sensitivity = range.width
    def scale = sensitivity / budget.epsilon

    if (budget.charge(budget.epsilon)) {
      doubleDelegate.sum() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def avg() : Double = {
    def sensitivity = range.width / doubleDelegate.count()
    def scale = sensitivity / budget.epsilon

    if (budget.charge(budget.epsilon)) {
      doubleDelegate.mean() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def min() : Double = {
    def sensitivity = range.width
    def scale = sensitivity / budget.epsilon

    if (budget.charge(budget.epsilon)) {
      doubleDelegate.min() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def max() : Double = {
    def sensitivity = range.width
    def scale = sensitivity / budget.epsilon

    if (budget.charge(budget.epsilon)) {
      doubleDelegate.max() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def median() : Double = {
    def sensitivity = range.width
    def scale = sensitivity / budget.epsilon

    if (budget.charge(budget.epsilon)) {
      doubleDelegate.min() + SparkLaplace.draw(scale)
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

}
