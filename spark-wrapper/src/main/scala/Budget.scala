/**
 * Created by aheifetz on 4/26/17.
 */
class Budget (_epsilon: Double, _scale: Int) {

  private var _value = _epsilon * _scale

  def epsilon = _epsilon
  def hasRemaining : Boolean = { _value > 0 }
  def charge(cost : Double) : Unit = { if (cost > 0) _value -= cost }
}
