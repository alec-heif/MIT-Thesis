package org.me.PrivateSpark

class Pair_Enforcement[K] (
                    _keys: Seq[K]
                    , _ranges : Map[K, api.Range]
                    ) extends Serializable {

  def keys : Seq[K] = _keys
  def ranges : Map[K, api.Range] = _ranges

  def set(_keys : Seq[K]) : Pair_Enforcement[K] = {
    new Pair_Enforcement[K](_keys, _ranges)
  }
  def set(_ranges : Map[K, api.Range]) : Pair_Enforcement[K] = {
    new Pair_Enforcement[K](_keys, _ranges)
  }
}

object Pair_Enforcement {
  def default[K]() : Pair_Enforcement[K] = {
    new Pair_Enforcement[K](Seq.empty[K], Map.empty[K, api.Range])
  }
}
