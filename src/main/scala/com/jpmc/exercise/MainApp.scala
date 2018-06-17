package com.jpmc.exercise

import org.apache.spark.sql.SparkSession

/**
  * Created by adnan_saqib on 16/06/2018.
  */
object MainApp extends App{

  val appName = "weather-data-analyzer"
  val master = "local[*]"
  val spark: SparkSession = SparkSession
    .builder()
      .config("spark.sql.shuffle.partitions","2") //optimised for standalone mode
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .appName(appName)
            .master(master)
              .getOrCreate()

  val stationListFile: String = "station-list.dat"
  val resultsOutputPath: String = args(0)

  val weatherDataAnalyzer = new WeatherDataAnalyzer(spark)

  val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)

  val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

  weatherDataAnalyzer.load(weatherDataTransformed, resultsOutputPath)

}
