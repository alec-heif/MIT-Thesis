package org.me.PrivateSpark

final class QueryInfo[K] (
                           val keys : Seq[K]
                           , val outputs: Int = 1
                           , val ranges : Map[K, api.Range] = Map.empty[K, api.Range]
                           ) extends Serializable {
  def set(_ranges : Map[K, api.Range]) : QueryInfo[K] = {
    new QueryInfo(keys, outputs, _ranges)
  }

  def set(_outputsPerInput : Int) : QueryInfo[K] = {
    new QueryInfo(keys, _outputsPerInput, ranges)
  }

  def add(key : K, range : api.Range) : QueryInfo[K] = {
    set(ranges + (key -> range))
  }

  def chain(newOutputs : Int) : QueryInfo[K] = {
    set(outputs * newOutputs)
  }
}

