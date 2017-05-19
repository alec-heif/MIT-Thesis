package org.me.PrivateSpark

class Budget (_epsilon: Double, _scale: Int) extends Serializable {

  private var _value = _epsilon * _scale

  def epsilon = _epsilon
  def canCharge(input : Double) : Boolean = { _value >= input }
  def charge(cost : Double) : Boolean = {
    if (canCharge(cost)) {
      _value -= cost
      true
    } else {
      false
    }
  }
}
