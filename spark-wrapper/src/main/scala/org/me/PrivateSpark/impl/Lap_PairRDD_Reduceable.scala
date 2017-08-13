package org.me.PrivateSpark.impl

import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.me.PrivateSpark._
import org.me.PrivateSpark.api.{Lap_RDD, Lap_PairRDD, Range}

import scala.reflect.ClassTag

class Lap_PairRDD_Reduceable[K, V](
                                 delegate : RDD[(K, V)]
                                 , info : QueryInfo
                                 , enforcement : Pair_Enforcement[K]
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

    // If they've already set a Range for this key, keep it, otherwise use new one
    var newEnforcement = Single_Enforcement.default()
    if (enforcement.keys.contains(key)) {
      newEnforcement = newEnforcement.set(enforcement.ranges(key))
    }

    RDDCreator.create(selected, info, newEnforcement)
  }

  override def setKeys(keys: Seq[K])(implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    RDDCreator.create(delegate, info, enforcement.set(keys))
  }

  override def setRanges(ranges: Map[K, Range])(implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    RDDCreator.create(delegate, info, enforcement.set(ranges))
  }

  override def setRangeForKeys(keys: Seq[K], range: Range)
                              (implicit tag: ClassTag[V]): Lap_PairRDD[K, V] = {
    var ranges = enforcement.ranges
    for (k <- keys) {
      ranges += (k -> range)
    }
    RDDCreator.create(delegate, info, enforcement.set(ranges))
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
    def budget = info.budget
    def keys = enforcement.keys
    def ranges = enforcement.ranges

    val doubleDelegate = delegate.asInstanceOf[RDD[(K, Double)]]
    // val reducibleDelegate = new PairRDDFunctions(doubleDelegate)

    var sensitivities = Map.empty[K, Double]
    for (k <- keys) {
      val range = ranges(k)
      val sensitivity = math.max(math.abs(range.max), math.abs(range.min))
      sensitivities += (k -> sensitivity)
    }

    val scales = sensitivities.map(x => (x._1, x._2 / budget.epsilon))

    val unenforcedOutput = doubleDelegate.reduceByKey(_ + _).collect()

    val enforcedOutput = enforceAll(unenforcedOutput, enforcement)

    if (budget.charge(budget.epsilon * info.outputs)) {
      enforcedOutput.map(x => (x._1, x._2 + Laplace.draw(scales(x._1))))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  override def kAvg()(implicit tag : ClassTag[K]) : Seq[(K, Double)] = {
    def budget = info.budget
    def keys = enforcement.keys
    def ranges = enforcement.ranges

    val doubleDelegate = delegate.asInstanceOf[RDD[(K, Double)]]
    val reducibleDelegate = new PairRDDFunctions(doubleDelegate)

    val lengths = reducibleDelegate.countByKey()

    var sensitivities = Map.empty[K, Double]
    for (k <- keys) {
      val range = ranges(k)
      // If value not present, we still need to add a sensitivity
      var length : Long = 1
      if (lengths.contains(k)) {
        length = lengths(k)
      }
      // Since every key has an average in the range, the max effect is the "change" effect
      def maxEffect = range.width
      val sensitivity = maxEffect / length
      sensitivities += (k -> sensitivity)
    }

    val scales = sensitivities.map(x => (x._1, x._2 / budget.epsilon))

    val unenforcedOutput =
      reducibleDelegate.reduceByKey(_ + _).collect().map(x => (x._1, x._2 / lengths(x._1)))

    val enforcedOutput = enforceAll(unenforcedOutput, enforcement)

    if (budget.charge(budget.epsilon * info.outputs)) {
      enforcedOutput.map(x => (x._1, x._2 + Laplace.draw(scales(x._1))))
    } else {
      throw new IllegalStateException("Privacy budget exceeded!")
    }
  }

  // Key enforcement needs to happen for both Reduceable and NonReduceable RDDs!
  def enforceKeys(output : Seq[(K, Double)], keys : Seq[K]) : Seq[(K, Double)] = {
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

  def enforceAll(output : Seq[(K, Double)], enforcement : Pair_Enforcement[K]) : Seq[(K, Double)] = {
    def keys = enforcement.keys
    def ranges = enforcement.ranges
    // Use sets to randomize order
    val expectedKeys = keys.toSet
    val outputSet = output.toSet

    // First get rid of all keys that are not expected
    val matcher = Utils.keyMatch(expectedKeys) _
    val trimmedOutputSet : Set[(K, Double)] = outputSet.filter(matcher)

    // Then add any keys that are expected but are missing
    // NOTE that unlike the keys-only enforcement, we use a random value in the range
    val outputKeys = trimmedOutputSet.map(x => x._1)
    val missingKeys = expectedKeys.diff(outputKeys)
    val missingOutputSet : Set[(K, Double)] = missingKeys.map(x => (x, ranges(x).random()))
    val finalOutputSet = trimmedOutputSet ++ missingOutputSet

    // Finally convert back to Seq
    val finalOutputSeq = finalOutputSet.toSeq

    def enforcedOutput = finalOutputSeq.map(elem => {
      def range = ranges(elem._1)
      (elem._1, range.enforce(elem._2))
    })
    enforcedOutput
  }
}

