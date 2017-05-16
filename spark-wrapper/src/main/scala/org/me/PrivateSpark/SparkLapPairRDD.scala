package org.me.PrivateSpark

import org.apache.spark.rdd.{PairRDDFunctions, RDD}

import scala.reflect.{ClassTag, classTag}

trait SparkLapPairRDD[K, V] extends Serializable {
  def mapValues[T : ClassTag](f : V => T, ranges : Map[K, Range]) : SparkLapPairRDD[K, T]
  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)]
  def cache() : Unit
}

object SparkLapPairRDD {
  def create[K, V : ClassTag]
  (delegate : RDD[(K, V)], info : QueryInfo[K])
  (implicit tag : ClassTag[V])
  : SparkLapPairRDD[K, V] = {
    // Need to enforce every potentially reducible creation
    val matcher = Utils.keyMatch(info.ranges.keySet) _
    val enforcer = Utils.enforce(info.ranges) _
    delegate match {
      case reducible: RDD[(K, Double)@unchecked] if tag == classTag[Double] => {
        val _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
        val result = new ReducibleSparkLapPairRDD[K, Double](_delegate.filter(matcher).map(enforcer), info)
        result.asInstanceOf[SparkLapPairRDD[K, V]]
      }
      case _ => new UnreducibleSparkLapPairRDD[K, V](delegate, info)
    }
  }
}

class UnreducibleSparkLapPairRDD[K, V](
                                        delegate : RDD[(K, V)]
                                        , info : QueryInfo[K]
                                        ) extends SparkLapPairRDD[K, V] {

  override def mapValues[T](f : V => T, ranges : Map[K, Range])(implicit tag : ClassTag[T]): SparkLapPairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)

    def h(f : V => T)(input : (K, V)) : (K, T) = {
      (input._1, f(input._2))
    }

    SparkLapPairRDD.create(delegate.map(h(g)), info.set(ranges))
  }

  override def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    throw new UnsupportedOperationException("Not permitted!")
  }

  override def cache() : Unit = {
    delegate.cache()
  }
}

class ReducibleSparkLapPairRDD[K, V](
                                   delegate : RDD[(K, V)]
                                   , info : QueryInfo[K]
                                   ) extends SparkLapPairRDD[K, V] {

  override def mapValues[T](f : V => T, ranges : Map[K, Range])(implicit tag : ClassTag[T]): SparkLapPairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)
    def h(f : V => T)(input : (K, V)) : (K, T) = {
      (input._1, f(input._2))
    }
    SparkLapPairRDD.create(delegate.map(h(g)), info.set(ranges))
  }

  def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def _delegate = delegate.asInstanceOf[RDD[(K, Double)]]
    def countableDelegate = _delegate.mapValues(x => 1.0)
    def func = new PairRDDFunctions(countableDelegate)
    def noise(input : (K, Double)) : (K, Double) = {
      (input._1, input._2 + SparkLaplace.draw(scale))
    }
    def scale = 1 / 0.1
    func.reduceByKey(_ + _).collect().map(noise)
  }

  override def cache() : Unit = {
    delegate.cache()
  }
}

