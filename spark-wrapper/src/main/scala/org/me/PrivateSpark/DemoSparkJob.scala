package org.me.PrivateSpark

import org.me.PrivateSpark.api.{SAR_RDD, Lap_RDD, PrivateSparkContext, Range}

object DemoSparkJob extends Serializable {

  def main(args: Array[String]): Unit = {
    average_netflix_rating()
  }

  def start_sar(): Unit = {
    println("\nStarting SparkSAR!" + "\n")
    val logFile = "file:///home/spark-user/thesis_repo/health_data.csv"
    val sc = new PrivateSparkContext("Simple App")

    def rdd = sc.getSarRDD(logFile)

    run_sar(rdd)
  }

  def start_lap() {
    println("\nStarting SparkLap!" + "\n")

    val logFile = "health_data.csv" // Should be some file on your system
    val sc = new PrivateSparkContext("Simple Application")

    def rdd = sc.getLapRDD(logFile)

    println("\n***************************PERTURBED OUTPUT********************************* " + "\n")
    run_lap(rdd)

    println("\n***************************ACTUAL OUTPUT********************************* " + "\n")
    // DEMO: this line disables privacy enforcement!
    Laplace.setEnabled(false)
    run_lap(rdd)

    sc.stop()
  }

  def average_netflix_rating() : Unit = {
    println("\nStarting SparkLap!" + "\n")

    val logFile = "file:///data/dig/spark/netflix/result_all.csv" // Should be some file on your system
    val sc = new PrivateSparkContext("Netflix Analysis")
    val lap_rdd = sc.getLapRDD(logFile)

//    println("\n***************************PERTURBED OUTPUT********************************* " + "\n")
//    run_netflix(lap_rdd)
//
//    println("\n***************************ACTUAL OUTPUT********************************* " + "\n")
//    Laplace.setEnabled(false)
//    run_netflix(lap_rdd)

    val sar_rdd = sc.getSarRDD(logFile)
    def split_rdd = sar_rdd.map( line => {
      def row = line.split(',')
      def cleaned_row = row.map(col => col.toLowerCase().trim())
      cleaned_row
    })
    def ratings = split_rdd.map(x => x(2).toDouble)
    def median_rating = ratings.median()
    println("Median rating: " + median_rating + "\n")
  }

  def run_netflix(rdd : Lap_RDD[String]) : Unit = {
    println("Total Ratings: " + rdd.count().toLong + "\n")
    val split = split_rdd(rdd)
    def ratings = rdd.map(x => x(2).toDouble, new Range(0, 5))
    println("Average Ratings" + ratings.avg() + "\n")
  }

  def run_sar(rdd: SAR_RDD[String]) : Unit = {
    def split_rdd = rdd.map( line => {
      def row = line.split(',')
      def cleaned_row = row.map(col => col.toLowerCase().trim())
      cleaned_row
    })
//    def heroin_deaths = split_rdd.filter(x => x(cols("heroin")).equals("y"))
    def heroin_deaths = split_rdd
    def heroin_ages = heroin_deaths.map(get_age)
//    def reduced_age = heroin_ages.reduce((left, right) => {
//      math.max(left, right)
//    })
    def median_age = heroin_ages.median()
    println("Median age of heroin death: " + median_age + "\n")

  }

  def run_lap(rdd : Lap_RDD[String]) : Unit = {

    demo_count(rdd)

    def split = split_rdd(rdd)
    split.cache()

    demo_map_filter(split)

//    demo_adversary_basic(split)

//    demo_adversary_clever(split)

//    demo_adversary_budget(split)

//    demo_group_by(split)

//    demo_multi_group(split)


  }

  def demo_count(rdd : Lap_RDD[String]) : Unit = {
    println("Total deaths : " + rdd.count() + "\n")
  }

  def demo_map_filter(rdd : Lap_RDD[Array[String]]) : Unit = {

    def heroin_deaths = rdd.filter(x => x(cols("heroin")).equals("y"))
    println("Deaths by heroin: " + heroin_deaths.count() + "\n")


    def heroin_ages = heroin_deaths.map(get_age, age_range)
    println("Average age of heroin death: " + heroin_ages.avg() + "\n")

  }

  def demo_adversary_basic(rdd : Lap_RDD[Array[String]]) : Unit = {
    def target_rdd = rdd
      .filter(x => x(cols("date")).equals("11/22/2016"))
      .filter(x => x(cols("city")).equals("middletown"))

    def target_count = target_rdd.count()
    print_if_nonzero(target_count, "drug user")

    def is_fentanyl_user_rdd = target_rdd.filter(x => x(cols("fentanyl")).equals("y"))
    def is_cocaine_user_rdd = target_rdd.filter(x => x(cols("cocaine")).equals("y"))

    print_if_nonzero(is_fentanyl_user_rdd.count(), "fentanyl user")
    print_if_nonzero(is_cocaine_user_rdd.count(), "cocaine user")
    println("")
  }

  def demo_group_by(rdd : Lap_RDD[Array[String]]) : Unit = {

    def counties_of_interest = List("hartford", "new haven", "new london", "tolland")
    def deaths_by_county = rdd.groupBy(x => x(cols("death_county")), counties_of_interest)

    println("Deaths by county: ")
    deaths_by_county.kCount().foreach(x => println(x._1 + ", " + x._2))
    println("")

    def ages_by_county = deaths_by_county.mapValues(get_age, age_range)

    println("Ages by county: ")
    ages_by_county.kAvg().foreach(x => println(x._1 + ", " + x._2))
    println("")

  }

  def demo_multi_group(rdd : Lap_RDD[Array[String]]) : Unit = {
    def drug_cols = cols.filter(input => input._2 > 14)
    def num_outputs = drug_cols.size

    def drug_type_grouper(input : Array[String]) : List[(String, Array[String])] = {

      var output = List[(String, Array[String])]()

      for (x <- drug_cols) {
        if (input(x._2).equals("y")) {
          def pair : (String, Array[String]) = (x._1, input)
          output = pair :: output
        }
      }

      output

    }

    def deaths_by_drug_type = rdd.groupByMulti(drug_type_grouper, drug_cols.keys.toSeq, num_outputs)
    println("Deaths by drug type: ")
    deaths_by_drug_type.kCount().foreach(x => println(x._1 + ", " + x._2))
    println("")

    def ages_by_drug_type = deaths_by_drug_type.mapValues(get_age, age_range)
    println("Ages by drug type: ")
    ages_by_drug_type.kAvg().foreach(x => println(x._1 + ", " + x._2))
    println("")

  }

  def demo_adversary_clever(rdd : Lap_RDD[Array[String]]) : Unit = {
    def amplify(x : Boolean) : Double = {
      if (x) {
        100000.0
      } else {
        0.0
      }
    }

    def target_rdd = rdd.filter(x => is_target(x))

    def fentanyl_user_rdd = target_rdd.map(x => amplify(is_fentanyl_user(x)))
    def cocaine_user_rdd = target_rdd.map(x => amplify(is_cocaine_user((x))))

    print_if_nonzero(fentanyl_user_rdd.sum(), "fentanyl user")
    print_if_nonzero(cocaine_user_rdd.sum(), "cocaine user")
    println("")

    def fentanyl_user_rdd_ranged = target_rdd.map(x => amplify(is_fentanyl_user(x)), new Range(0, 1))
    def cocaine_user_rdd_ranged = target_rdd.map(x => amplify(is_cocaine_user((x))), new Range(0, 1))

    print_if_nonzero(fentanyl_user_rdd_ranged.sum(), "fentanyl user")
    print_if_nonzero(cocaine_user_rdd_ranged.sum(), "cocaine user")
    println("")
  }

  def demo_adversary_budget(rdd : Lap_RDD[Array[String]]) : Unit = {
    def amplify(x : Boolean) : Double = {
      if (x) {
        100000.0
      } else {
        0.0
      }
    }

    def target_rdd = rdd.filter(x => is_target(x))

    def fentanyl_user_rdd = target_rdd.map(x => amplify(is_fentanyl_user(x)), new Range(0, 100000))

    for (i <- 0 to 200) {
      println("Fentanyl user sum: " + fentanyl_user_rdd.sum())
    }
    println("")
  }

  def print_if_nonzero(count : Double, message : String) : Unit = {
    if (count > 0) {
      println ("Target is " + message + " with value " + count)
    } else {
      println("Target is not " + message + " with value " + count)
    }
  }

  def split_rdd(rdd : Lap_RDD[String]) : Lap_RDD[Array[String]] = {
    rdd.map( line => {
      def row = line.split(',')
      def cleaned_row = row.map(col => col.toLowerCase().trim())
      cleaned_row
    })
  }

  def age_range = new Range(10, 90)
  val get_age = (x : Array[String]) => { x(cols("age")).toDouble }

  def is_target(x : Array[String]) : Boolean = {
    x(cols("date")).equals("11/22/2016") && x(cols("city")).equals("middletown")
  }

  def is_cocaine_user(x : Array[String]) : Boolean = {
    x(cols("cocaine")).equals("y")
  }

  def is_fentanyl_user(x : Array[String]) : Boolean = {
    x(cols("fentanyl")).equals("y")
  }

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

}

