package org.me.PrivateSpark.api

import scala.util.Random

class Range(_min: Double = Double.MinValue, _max: Double = Double.MaxValue) extends Serializable {
  val min = _min
  val max = _max
  val width = max - min

  private val _random = new Random()

  def random() : Double = _random.nextDouble() * width + min

  def enforce(x : Double) : Double = {
    if (x < min || x > max) {
      // println("ENFORCING! " + x)
      random()
    } else {
      x
    }
  }
}

