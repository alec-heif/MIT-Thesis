package org.me.PrivateSpark.impl

import org.apache.spark.rdd.RDD
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_RDD, Lap_PairRDD, Range}

import scala.reflect.ClassTag

class Lap_PairRDD_NonReduceable [K, V](
                                   delegate : RDD[(K, V)]
                                   , info : QueryInfo
                                   , enforcement: Pair_Enforcement[K]
                                   ) extends Lap_PairRDD[K, V] {

  override def cache() : Unit = {
    delegate.cache()
  }

  override def mapValues[T](f : V => T)(implicit tag : ClassTag[T]): Lap_PairRDD[K, T] = {
    val g = Cleaner.enforcePurity(f)

    def h(input : (K, V)) : (K, T) = (input._1, g(input._2))

    RDDCreator.create(delegate.map(h), info, enforcement)
  }

  override def filterValues(f : V => Boolean)(implicit tag : ClassTag[V]) : Lap_PairRDD[K, V] = {
    val g = Cleaner.enforcePurity(f)

    def h(input : (K, V)) : Boolean = g(input._2)

    RDDCreator.create(delegate.filter(h), info, enforcement)

  }

  override def get(key : K)(implicit tag : ClassTag[V]) : Lap_RDD[V] = {
    // Extract the relevant RDD. Note: Might take a while...
    val selected = delegate.filter(x => x._1.equals(key)).map(x => x._2)

    // Don't bother saving the range since it's non-reduceable
    RDDCreator.create(selected, info, Single_Enforcement.default())
  }

  override def setKeys(keys: Seq[K])(implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    RDDCreator.create(delegate, info, enforcement.set(keys))
  }

  override def setRanges(ranges: Map[K, Range])(implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    throw new UnsupportedOperationException("Only Pair_RDDs with Double values need Ranges!")
  }

  override def setRangeForKeys(keys: Seq[K], range: Range)
                              (implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    throw new UnsupportedOperationException("Only Pair_RDDs with Double values need Ranges!")
  }

  override def kCount()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def budget = info.budget
    def keys = enforcement.keys

    def sensitivity = 1.0
    def scale = sensitivity / budget.epsilon
    def countedDelegate = delegate.map(x => (x._1, 1.0))

    if (budget.charge(budget.epsilon * info.outputs)) {
      val unenforcedOutput = countedDelegate.reduceByKey(_ + _).collect()
      val enforcedOutput = enforceKeys(unenforcedOutput, keys)
      enforcedOutput.map(Utils.noise(scale))
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

  // Key enforcement needs to happen for both Reduceable and NonReduceable RDDs!
  def enforceKeys(output : Seq[(K, Double)], keys : Seq[K])
                                   : Seq[(K, Double)] = {
    // Use sets to randomize order
    val expectedKeys = keys.toSet
    val outputSet = output.toSet

    // First get rid of all keys that are not expected
    val matcher = Utils.keyMatch(expectedKeys) _
    val trimmedOutputSet : Set[(K, Double)] = outputSet.filter(matcher)

    // Then add any keys that are expected but are missing
    val outputKeys = trimmedOutputSet.map(x => x._1)
    val missingKeys = expectedKeys.diff(outputKeys)
    val missingOutputSet : Set[(K, Double)] = missingKeys.map(x => (x, 0.0))
    val finalOutputSet = trimmedOutputSet ++ missingOutputSet

    // Finally convert back to Seq
    finalOutputSet.toSeq
  }
}
