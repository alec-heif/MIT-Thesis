package org.me.PrivateSpark

/**
 * Created by aheifetz on 5/3/17.
 */
class Range(_min: Double = Double.MinValue, _max: Double = Double.MaxValue) {

  def min = _min
  def max = _max
  def width = max - min

}
