package org.me.PrivateSpark

import scala.util.Random

object Laplace extends Serializable {
  def rand = new Random()
  def location = 0

  var enabled = true

  def setEnabled(_enabled : Boolean) = {
    enabled = _enabled
  }

  def getEnabled = enabled

  def draw(scale : Double) : Double = {
    val u = rand.nextDouble()
    if (enabled) {
      if (u < 0.5) {
        location + scale * math.log(2 * u)
      } else {
        location - scale * math.log(2 * (1 - u))
      }
    } else {
      0.0
    }
  }
}
