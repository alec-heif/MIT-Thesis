package org.me.PrivateSpark

object Utils {

  def trim[T, K, V]
  (f : (T => Seq[(K, V)]), maxOutputs : Int)
  (input : T)
  : Seq[(K, V)] = {
    f(input)
      .take(maxOutputs)

  }

  def enforce(range : api.Range) (input : Double) : Double = {
    range.enforce(input)
  }

  def enforce[K] (ranges : Map[K, api.Range]) (input : (K, Double)) : (K, Double) = {
    (input._1, enforce(ranges(input._1))(input._2))
  }

  def keyMatch[K, V] (keys : Set[K]) (input : (K, V)) : Boolean = {
    keys.contains(input._1)
  }

  def noise[K] (scale : Double) (input : (K, Double)) : (K, Double) = {
    (input._1, input._2 + Laplace.draw(scale))
  }

}
