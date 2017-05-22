package org.me.PrivateSpark

import org.me.PrivateSpark.api.{Lap_RDD, PrivateSparkContext, Range}

object DemoSparkJob extends Serializable {

  val cols = Map[String, Int](
  "date" -> 1
  , "sex" -> 2
  , "race" -> 3
  , "age" -> 4
  , "city" -> 5
  , "state" -> 6
  , "county" -> 7
  , "death_city" -> 8
  , "death_state" -> 9
  , "death_county" -> 10
  , "cause" -> 14
  , "heroin" -> 15
  , "cocaine" -> 16
  , "fentanyl" -> 17
  , "oxycodone" -> 18
  , "oxymorphone" -> 19
  , "etoh" -> 20
  , "hydrocodeine" -> 21
  , "benzodiazepine" -> 22
  , "methadone" -> 23
  , "methamphetamine" -> 24
  , "tramadine" -> 25
  , "morphine" -> 26
  )

  def main(args: Array[String]) {
    val logFile = "health_data.csv" // Should be some file on your system
    val sc = new PrivateSparkContext("Simple Application")

    def rdd = sc.getLapRDD(logFile)

    println("\n***************************PERTURBED OUTPUT********************************* " + "\n")
    run(rdd)

    // DEMO: Uncomment these lines to disable privacy checks!
    println("\n***************************ACTUAL OUTPUT********************************* " + "\n")
    Laplace.setEnabled(false)
    run(rdd)

    sc.stop()
  }

  def run(rdd : Lap_RDD[String]) : Unit = {
    def split = rdd.map(x => x.split(',').map(x => x.toLowerCase().trim()))
    split.cache()

    def heroin_deaths = split.filter(x => {
      x(cols("heroin")).equals("y")
    })
    println("Total deaths : " + rdd.count() + "\n")

    println("Deaths by heroin: " + heroin_deaths.count() + "\n")

    def age_range = new Range(10, 90)
    val get_age = (x : Array[String]) => { x(cols("age")).toDouble }

    def heroin_ages = heroin_deaths.map(get_age, age_range)
    println("Average age of heroin death: " + heroin_ages.avg() + "\n")

    def deaths_by_county = split.groupBy(x => x(cols("death_county")),
      List("hartford", "new haven", "new london", "tolland"))

    println("Deaths by county: ")
    deaths_by_county.kCount().foreach(x => println(x._1 + ", " + x._2))
    println("")

    def ages_by_county = deaths_by_county.mapValues(get_age, age_range)
    println("Ages by county: ")
    ages_by_county.kAvg().foreach(x => println(x._1 + ", " + x._2))
    println("")

    def drug_cols = cols.filter(input => input._2 > 14)
    def num_outputs = drug_cols.size
    val drug_type_grouper = (input : Array[String]) => {

      var result = List[(String, Array[String])]()

      for (x <- drug_cols) {
        if (input(x._2).equals("y")) {
          def pair : (String, Array[String]) = (x._1, input)
          result = pair :: result
        }
      }
      result
    }

    def deaths_by_drug_type = split.groupByMulti(drug_type_grouper, drug_cols.keys.toSeq, num_outputs)
    println("Deaths by drug type: ")
    deaths_by_drug_type.kCount().foreach(x => println(x._1 + ", " + x._2))
    println("")

    def ages_by_drug_type = deaths_by_drug_type.mapValues(get_age, age_range)
    println("Ages by drug type: ")
    ages_by_drug_type.kAvg().foreach(x => println(x._1 + ", " + x._2))
    println("")
  }
}

