package org.me.PrivateSpark.api

import scala.reflect.ClassTag

trait Lap_RDD[T] extends Serializable {

  def cache() : Unit

  /*
      Takes in a function and applies it to each element in the RDD, returning a new
      RDD consisting of the results of the function after being applied to each element.

      @param f Pure function applied to each element of the RDD.
      @param range Optional range of output values, only required if U is of type Double.
      @return non-pair RDD consisting of all and only the results of f with length unchanged.
   */
  def map[U : ClassTag] (
                          f : T => U,
                          range : Range = new Range()
                          ) : Lap_RDD[U]

  /*
      Takes in a function and applies it to each element in the RDD, returning a new
      RDD consisting of all and only elements of the RDD for which the function returns true.

      @param f Pure function applied to each element of the RDD.
      @return non-pair RDD consisting of all and only the elements in the original RDD for which f
          returns true.
   */
  def filter(
              f : T => Boolean
              ) (implicit tag : ClassTag[T]) : Lap_RDD[T]

  def groupBy[K] ( f : T => K , keys : Seq[K] ) (implicit tag : ClassTag[T]) : Lap_PairRDD[K, T]

  def groupByMulti[K, V : ClassTag] (
                                 f : T => Seq[(K, V)]
                                 , keys : Seq[K]
                                 , maxOutputs : Int = 1
                                 , ranges : Map[K, Range] = Map.empty[K, Range]
                                 ) : Lap_PairRDD[K, V]


  def count() : Double
  def sum() : Double
  def avg() : Double
  def min() : Double
  def max() : Double
  def median() : Double
}
