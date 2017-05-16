package org.me.PrivateSpark

import scala.util.Random

object SparkLaplace extends Serializable {
  def rand = new Random()
  def location = 0

  def draw(scale : Double) : Double = {
    val u = rand.nextDouble()
    if (u < 0.5) {
      location + scale * math.log(2 * u)
    } else {
      location - scale * math.log(2 * (1 - u))
    }
  }
}
