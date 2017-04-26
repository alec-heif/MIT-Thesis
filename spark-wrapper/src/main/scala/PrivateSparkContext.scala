import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class PrivateSparkContext (conf: SparkConf) {
  // TODO load this from a config file defined on the system
  private val EPSILON = 0.05
  private val SCALE = 1

  private val _budget = new Budget(EPSILON, SCALE)
  private val ctx = new SparkContext(conf)

  def budget = _budget

  def textFile(path: String) : RDD[String] = {
    if (budget.hasRemaining) {
      budget.charge(budget.epsilon)
      ctx.textFile(path)
    } else {
      ctx.emptyRDD
    }
  }

  def stop() : Unit = { ctx.stop() }
}
