package org.me.PrivateSpark

final class QueryInfo[K] (
                     val outputs: Int = 1
                     , val ranges : Map[K, Range] = Map.empty[K, Range]
                     ) {
  def set(_ranges : Map[K, Range]) : QueryInfo[K] = {
    new QueryInfo(outputs, _ranges)
  }

  def set(_outputsPerInput : Int) : QueryInfo[K] = {
    new QueryInfo(_outputsPerInput, ranges)
  }

  def add(key : K, range : Range) : QueryInfo[K] = {
    set(ranges + (key -> range))
  }

  def chain(newOutputs : Int) : QueryInfo[K] = {
    set(outputs * newOutputs)
  }
}

object QueryInfo {
  def default[K]() : QueryInfo[K] = {
    new QueryInfo[K]()
  }
}
