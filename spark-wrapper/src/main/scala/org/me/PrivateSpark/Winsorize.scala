package org.me.PrivateSpark

import scala.util.Random

object Winsorize extends Serializable {

  def rand = new Random()

  def private_quantile(data : Seq[Double], quantile : Double, epsilon : Double) : Double = {
    val k = data.length

    val sorted_data = data.sortBy(x => x)
    // TEST
    val diffed_data = get_diffs(sorted_data)

    def utility_function(element : (Int, Double)) = {
      val (index, diff) = element
      def exponent =  math.exp(-1 * epsilon * math.abs(index - quantile * k))
      (index, diff * exponent)
    }
    val utilities = diffed_data.map(utility_function)

    val probabilities = normalize(utilities)

    val selected_idx = choose_probability(probabilities)

    val lower_bound : Double = sorted_data(selected_idx)
    val width : Double = diffed_data(selected_idx)._2
    // Uniform draw between z_i and z_i+1
    val result = lower_bound + rand.nextDouble()*width

    result

  }

  def choose_probability(probs : Seq[(Int, Double)]) : Int = {
    var random = rand.nextDouble()
    probs.foreach {
      case (index, probability) => {
        if (probability > random) {
          return index
        }
        random = random - probability
      }
    }
    probs.length - 1
  }

  def normalize(utilities : Seq[(Int, Double)]) : Seq[(Int, Double)] = {
    def sum = utilities.foldLeft(0.0){(acc, next) => acc + next._2 }
    def probs = utilities.map{elem => (elem._1, elem._2 / sum) }
    probs
  }

  /**
   * Given a list of elements, return a list where index i maps to data(i+1) - data(i)
   * @param sorted_data
   * @return
   */
  def get_diffs(sorted_data : Seq[Double]) : Seq[(Int, Double)] = {
    def padded_data = sorted_data :+ sorted_data.max
    def diffed_data = (padded_data drop 1, padded_data).zipped.map(_-_)
    def indexed_diffed_data = diffed_data.zipWithIndex.map{case (k,v) => (v,k)}
    indexed_diffed_data
  }

}
