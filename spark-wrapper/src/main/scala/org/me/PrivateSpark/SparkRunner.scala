package org.me.PrivateSpark

import java.io.File

import org.apache.spark.launcher.SparkLauncher

class SparkRunner(_jar : String, _main : String, _outputPath : String) {

  def jar = _jar
  def main = _main
  def outputPath = _outputPath

  def start(): Unit = {
    val file = new File(outputPath)
    var launcher = new SparkLauncher()
      .setAppResource(jar)
      .setMainClass(main)
      .addJar("/Users/aheifetz/Dropbox/MIT Classes/Thesis/impl/code/spark-wrapper/target/scala-2.11/wrapper-2_2.11-1.0.jar")
      .setMaster("local")
      .setSparkHome("/Users/aheifetz/Dropbox/MIT Classes/Thesis/impl/code/spark-bin")
      .setAppName("DemoApp")
      .redirectOutput(file)
      .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
      .startApplication()
  }
}
