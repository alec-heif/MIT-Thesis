package org.me.PrivateSpark

import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.{ClassTag, classTag}

trait SparkLapPairRDD[K, V] extends Serializable {
  def mapValues[T : ClassTag](f : V => T, ranges : Map[K, Range]) : SparkLapPairRDD[K, T]
  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)]
  def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)]
  def cache() : Unit
}

object SparkLapPairRDD {
  def create[K, V : ClassTag]
  (delegate : RDD[(K, V)], budget : Budget, info : QueryInfo[K])
  (implicit tag : ClassTag[V])
  : SparkLapPairRDD[K, V] = {
    // Need to enforce every potentially reducible creation
    val matcher = Utils.keyMatch(info.keys.toSet) _
    val enforcer = Utils.enforce(info.ranges) _
    delegate match {
      case reducible: RDD[(K, Double)@unchecked] if tag == classTag[Double] =>
        val _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
        val result =
          new ReducibleSparkLapPairRDD[K, Double](_delegate.filter(matcher).map(enforcer), budget, info)
        result.asInstanceOf[SparkLapPairRDD[K, V]]
      case _ => new UnreducibleSparkLapPairRDD[K, V](delegate.filter(matcher), budget, info)
    }
  }
}

class UnreducibleSparkLapPairRDD[K, V](
                                        delegate : RDD[(K, V)]
                                        , budget : Budget
                                        , info : QueryInfo[K]
                                        ) extends SparkLapPairRDD[K, V] {

  override def mapValues[T](f : V => T, ranges : Map[K, Range])(implicit tag : ClassTag[T]): SparkLapPairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)

    def h(f : V => T)(input : (K, V)) : (K, T) = {
      (input._1, f(input._2))
    }

    SparkLapPairRDD.create(delegate.map(h(g)), budget, info.set(ranges))
  }

  override def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def countedDelegate = delegate.map(x => (x._1, 1.0))

    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    if (budget.charge(budget.epsilon * info.keys.size)) {
      countedDelegate.reduceByKey(_ + _).collect().map(Utils.noise(scale))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    throw new UnsupportedOperationException("Not permitted!")
  }

  override def cache() : Unit = {
    delegate.cache()
  }
}

class ReducibleSparkLapPairRDD[K, V](
                                   delegate : RDD[(K, V)]
                                   , budget : Budget
                                   , info : QueryInfo[K]
                                   ) extends SparkLapPairRDD[K, V] {

  override def mapValues[T](f : V => T, ranges : Map[K, Range])(implicit tag : ClassTag[T]): SparkLapPairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)
    def h(f : V => T)(input : (K, V)) : (K, T) = {
      (input._1, f(input._2))
    }
    SparkLapPairRDD.create(delegate.map(h(g)), budget, info.set(ranges))
  }

  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    def countedDelegate = delegate.map(x => (x._1, 1.0))
    if (budget.charge(budget.epsilon * info.keys.size)) {
      countedDelegate.reduceByKey(_ + _).collect().map(Utils.noise[K](scale))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }


  def kSum()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def doubleDelegate = delegate.asInstanceOf[RDD[(K, Double)]]

    def scale = 1.0 / budget.epsilon
    def reducibleDelegate = new PairRDDFunctions(doubleDelegate)
    reducibleDelegate.reduceByKey(_ + _).collect().map(x => (x._1, x._2 + SparkLaplace.draw(scale)))
  }

  override def cache() : Unit = {
    delegate.cache()
  }
}

