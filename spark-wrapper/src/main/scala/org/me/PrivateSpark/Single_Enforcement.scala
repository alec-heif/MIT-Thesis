package org.me.PrivateSpark

class Single_Enforcement (_range : api.Range) extends Serializable {

  def range : api.Range = _range

  def set(_range : api.Range) : Single_Enforcement = {
    new Single_Enforcement(_range)
  }
}

object Single_Enforcement {
  def default() : Single_Enforcement = {
    new Single_Enforcement(new api.Range())
  }
}
