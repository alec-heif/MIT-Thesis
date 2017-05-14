package org.me.PrivateSpark

import java.lang.reflect.{Method, Modifier}

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SparkLapRDD[T: ClassTag](
                                _delegate: RDD[T],
                                _budget: Budget,
                                _range: Range = new Range()
                                ) extends Serializable {

  private val delegate = _delegate
  def budget = _budget
  def range = _range

  def enforcePurity[T: ClassTag, U: ClassTag](f : T => U): (T => U) = {
    var foo : Class[_] = f.getClass
    // Purge things accessible by enclosure
    while (foo != null) {
      foo.getDeclaredFields.foreach(field => {
        println(field)
        field.setAccessible(true)
        if (!Modifier.isFinal(field.getModifiers)) {
          def fieldName = field.getName
          throw new IllegalArgumentException("Field references non-final parameter " + fieldName)
        }
      })
      foo = foo.getEnclosingClass()
    }
    // Purge things accessible by inheritance
    foo = f.getClass
    while (foo != null) {
      foo.getDeclaredFields.foreach(field => {
        println(field)
        field.setAccessible(true)
        if (!Modifier.isFinal(field.getModifiers)) {
          def fieldName = field.getName
          throw new IllegalArgumentException("Field references non-final parameter " + fieldName)
        }
      })
      foo = foo.getSuperclass()
    }
    f
  }

  def enclose[T, U](enclosed: T)(func: T => U) : U = func(enclosed)

  def map[U: ClassTag](f: T => U, userRange: Range = range): SparkLapRDD[U] = {
    val g = enforcePurity(f)
    // Force eager evaluation
    new SparkLapRDD(delegate.map(g), budget, userRange)
  }

  def filter(f: T => Boolean): SparkLapRDD[T] = {
    enforcePurity(f)
    new SparkLapRDD(delegate.filter(f), budget, range)
  }

  def groupBy[K: ClassTag, V: ClassTag](
                            f: T => Seq[(K, V)],
                            userRange: Range = range)
  : SparkLapPairRDD[K, Iterable[V]] = {

    // TODO incorporate this logic into reduction phase
    /*
  def g(input: T): Seq[(K, V)] = {
    // Returned keys must be in the input
    def keyMatch(input: (K, V)): Boolean = {
      keys.contains(input._1)
    }

    // Apply key filter and truncate to length
    def result = f(input)
      .filter(keyMatch)
      .take(numVals)

    if (result.isEmpty) {
      return Seq.empty[(K,V)]
    }

    result.foreach(println)

    // Also need to pad to length if necessary, so choose random k/v to pad with
    def random = new Random
    def randKey = keys(random.nextInt(keys.length))
    def randVal = result(random.nextInt(result.length))._2

    // Apply padding and return
    result.padTo(numVals, (randKey, randVal))
    }
    */
    // TODO apply budget
    enforcePurity(f)
    new SparkLapPairRDD(delegate.flatMap(f).groupByKey(), budget, userRange)
  }

  // TODO remove
  def collect = delegate.collect
}
