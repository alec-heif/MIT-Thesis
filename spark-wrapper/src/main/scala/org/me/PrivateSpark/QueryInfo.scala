package org.me.PrivateSpark

class QueryInfo (
                           val _budget : Budget,
                           val _outputs : Int = 1
                           ) extends Serializable {

  def budget : Budget = _budget
  def outputs : Int = _outputs

  def scaleOutputs(_outputs : Int) : QueryInfo = {
    new QueryInfo(budget, outputs * _outputs)
  }
}

