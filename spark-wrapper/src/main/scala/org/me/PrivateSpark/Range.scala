package org.me.PrivateSpark

import scala.util.Random

/**
 * Created by aheifetz on 5/3/17.
 */
class Range(_min: Double = Double.MinValue, _max: Double = Double.MaxValue) extends Serializable {
  val min = _min
  val max = _max
  val width = max - min

  private val _random = new Random()

  def random() : Double = _random.nextDouble() * width + min

  def enforce(x : Double) : Double = {
    if (x < min || x > max) {
      random()
    } else {
      x
    }
  }
}

object Range {
  def default() : Range = {
    new Range()
  }
}
