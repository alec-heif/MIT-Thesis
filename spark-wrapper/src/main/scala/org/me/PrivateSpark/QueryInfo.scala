package org.me.PrivateSpark

final class QueryInfo[K] (
                           val keys : Seq[K]
                           , val outputs: Int = 1
                           , val ranges : Map[K, Range] = Map.empty[K, Range]
                           ) extends Serializable {
  def set(_ranges : Map[K, Range]) : QueryInfo[K] = {
    new QueryInfo(keys, outputs, _ranges)
  }

  def set(_outputsPerInput : Int) : QueryInfo[K] = {
    new QueryInfo(keys, _outputsPerInput, ranges)
  }

  def add(key : K, range : Range) : QueryInfo[K] = {
    set(ranges + (key -> range))
  }

  def chain(newOutputs : Int) : QueryInfo[K] = {
    set(outputs * newOutputs)
  }
}

object QueryInfo {
  def default[K](keys : Seq[K]) : QueryInfo[K] = {
    new QueryInfo[K](keys)
  }
}
