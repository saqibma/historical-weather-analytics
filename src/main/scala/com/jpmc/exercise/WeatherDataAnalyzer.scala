package com.jpmc.exercise

import java.io.File

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by adnan_saqib on 16/06/2018.
  */
class WeatherDataAnalyzer(val spark: SparkSession) extends Serializable {

  def extract(stationListFile : String): DataFrame = {
    val stationListRDD = spark.sparkContext.textFile(getResourcePath(stationListFile))
    val stationsRDD = stationListRDD.flatMap(line => line.split(",")).map(_.trim)
    val stationData = stationsRDD.map(station => (station,
      WeatherRestClient.get(s"https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/${station}data.txt")))
    val weatherDataRDD = stationData.flatMap(rec => {
        val station = rec._1
        val filterMeasures = rec._2.filter(line => (line.trim.startsWith("18") || line.trim.startsWith("19") || line.trim.startsWith("20")))
        val cleanedMeasures = filterMeasures.map(line =>
          line.replace("*","").replace("#","").replace("Provisional","")
          .replace("Change to Monckton Ave","").replace("||","").replace("$","").replace("all data from Whitby",""))
        cleanedMeasures.map(rec => {
          val measures = rec.split("\\s+")
          measures.length match {
            case x if x >= 8 => WeatherData(station,
              removeIncosistencies(measures(1)),
              removeIncosistencies(measures(2)),
              removeIncosistencies(measures(3)),
              removeIncosistencies(measures(4)),
              removeIncosistencies(measures(5)),
              removeIncosistencies(measures(6)),
              removeIncosistencies(measures(7)))
            case 7 => WeatherData(station,
              removeIncosistencies(measures(1)),
              removeIncosistencies(measures(2)),
              removeIncosistencies(measures(3)),
              removeIncosistencies(measures(4)),
              removeIncosistencies(measures(5)),
              removeIncosistencies(measures(6)),
              null)
            case _ => WeatherData(station,null,null,null,null,null,null,null)
          }

        })
      })
      import spark.implicits._
      val weatherDS : Dataset[WeatherData] = spark.createDataset(weatherDataRDD)

      val weatherDSWithColumnTypeCasted = weatherDS
        .withColumn("rainInMM", weatherDS("rainInMM").cast("double"))
        .withColumn("sunInHours", weatherDS("sunInHours").cast("double"))

      weatherDSWithColumnTypeCasted.persist()
      weatherDSWithColumnTypeCasted
  }

  def removeIncosistencies(measure : String) = {
    if(!"---".equals(measure.trim)) measure.trim else null
  }

  private final def getResourcePath(file: String): String = {
    new File(getClass.getClassLoader.getResource(file).getFile).getCanonicalPath
  }

  def transform(weatherDataExtracted: DataFrame): Map[String, DataFrame] = {
    val weatherDataTransformed = Map.newBuilder[String, DataFrame]

    val stationByCount = weatherDataExtracted
      .groupBy(col("station"))
        .count()
          .orderBy(col("count").desc, col("station"))

    weatherDataTransformed.+=("stationByCount" -> stationByCount)

    val stationByRainfall = weatherDataExtracted
      .groupBy("station")
        .max("rainInMM")
          .orderBy(col("max(rainInMM)").desc, col("station"))

    weatherDataTransformed.+=("stationByRainfall" -> stationByRainfall)

    val stationBySunshine = weatherDataExtracted
      .groupBy("station")
        .max("sunInHours")
          .orderBy(col("max(sunInHours)").desc, col("station"))

    weatherDataTransformed.+=("stationBySunshine" -> stationBySunshine)

    val worstRainfallByStation = weatherDataExtracted
      .groupBy("station")
        .min("rainInMM")
          .orderBy(col("min(rainInMM)").desc, col("station"))
            .select(col("station"),col("min(rainInMM)").alias("rainInMM"))

    val worstRainfallYearAndMonthByStation = weatherDataExtracted
      .join(broadcast(worstRainfallByStation), Seq("station", "rainInMM"))
        .select(weatherDataExtracted.col("station"),col("year"),col("month"),col("rainInMM"))

    weatherDataTransformed.+=("worstRainfallYearAndMonthByStation" -> worstRainfallYearAndMonthByStation)

    val bestSunshineByStation = weatherDataExtracted
      .groupBy("station").max("sunInHours")
        .orderBy(col("max(sunInHours)").desc, col("station"))
          .select(col("station"),col("max(sunInHours)").alias("sunInHours"))

    val bestSunshineYearAndMonthByStation = weatherDataExtracted
      .join(broadcast(bestSunshineByStation), Seq("station", "sunInHours"))
        .select(weatherDataExtracted.col("station"),col("year"),col("month"),col("sunInHours"))

    weatherDataTransformed.+=("bestSunshineYearAndMonthByStation" -> bestSunshineYearAndMonthByStation)

    val averageRainfallForMayAcrossAllStations = weatherDataExtracted
      .where(col("month") === 5)
        .groupBy(col("year"))
          .agg(avg(col("rainInMM")))
            .select(col("year"), round(col("avg(rainInMM)"), 2).alias("avg(rainInMM)"))
              .orderBy(col("avg(rainInMM)").desc)

    averageRainfallForMayAcrossAllStations.persist()

    weatherDataTransformed.+=("averageRainfallForMayAcrossAllStations" -> averageRainfallForMayAcrossAllStations)

    val bestAverageRainfallForMayAcrossAllStations = averageRainfallForMayAcrossAllStations.agg(max("avg(rainInMM)"))

    val bestYearForAverageRainfallForMayAcrossAllStations = averageRainfallForMayAcrossAllStations
      .join(broadcast(bestAverageRainfallForMayAcrossAllStations),
        averageRainfallForMayAcrossAllStations.col("avg(rainInMM)") === bestAverageRainfallForMayAcrossAllStations.col("max(avg(rainInMM))"))
        .select(col("year").alias("Year"), col("max(avg(rainInMM))").alias("Maximum Average Rain Across All Stations for May"))

    weatherDataTransformed.+=("bestYearForAverageRainfallForMayAcrossAllStations" -> bestYearForAverageRainfallForMayAcrossAllStations)

    val worstAverageRainfallForMayAcrossAllStations = averageRainfallForMayAcrossAllStations.agg(min("avg(rainInMM)"))

    val worstYearForAverageRainfallForMayAcrossAllStations = averageRainfallForMayAcrossAllStations
      .join(broadcast(worstAverageRainfallForMayAcrossAllStations),
        averageRainfallForMayAcrossAllStations.col("avg(rainInMM)") === worstAverageRainfallForMayAcrossAllStations.col("min(avg(rainInMM))"))
        .select(col("year").alias("Year"), col("min(avg(rainInMM))").alias("Minimum Average Rain Across All Stations for May"))

    weatherDataTransformed.+=("worstYearForAverageRainfallForMayAcrossAllStations" -> worstYearForAverageRainfallForMayAcrossAllStations)


    val averageSunshineForMayAcrossAllStations = weatherDataExtracted
      .where(col("month") === 5)
        .na.drop("all", Seq("sunInHours"))
          .groupBy(col("year"))
            .agg(avg(col("sunInHours")))
              .select(col("year"), round(col("avg(sunInHours)"), 2).alias("avg(sunInHours)"))
                .orderBy(col("avg(sunInHours)").desc)

    averageSunshineForMayAcrossAllStations.persist()

    weatherDataTransformed.+=("averageSunshineForMayAcrossAllStations" -> averageSunshineForMayAcrossAllStations)

    val bestAverageSunshineForMayAcrossAllStations = averageSunshineForMayAcrossAllStations.agg(max("avg(sunInHours)"))

    val bestYearForAverageSunshineForMayAcrossAllStations = averageSunshineForMayAcrossAllStations
      .join(broadcast(bestAverageSunshineForMayAcrossAllStations),
        averageSunshineForMayAcrossAllStations.col("avg(sunInHours)") === bestAverageSunshineForMayAcrossAllStations.col("max(avg(sunInHours))"))
        .select(col("year").alias("Year"), col("max(avg(sunInHours))").alias("Maximum Average Sunshine Across All Stations for May"))

    weatherDataTransformed.+=("bestYearForAverageSunshineForMayAcrossAllStations" -> bestYearForAverageSunshineForMayAcrossAllStations)

    val worstAverageSunshineForMayAcrossAllStations = averageSunshineForMayAcrossAllStations.agg(min("avg(sunInHours)"))

    val worstYearForAverageSunshineForMayAcrossAllStations = averageSunshineForMayAcrossAllStations
      .join(broadcast(worstAverageSunshineForMayAcrossAllStations),
        averageSunshineForMayAcrossAllStations.col("avg(sunInHours)") === worstAverageSunshineForMayAcrossAllStations.col("min(avg(sunInHours))"))
        .select(col("year").alias("Year"), col("min(avg(sunInHours))").alias("Minimum Average Sunshine Across All Stations for May"))

    weatherDataTransformed.+=("worstYearForAverageSunshineForMayAcrossAllStations" -> worstYearForAverageSunshineForMayAcrossAllStations)
    weatherDataTransformed.result()
  }

  def load(weatherDataTransformed: Map[String, DataFrame], path: String): Unit = {
    //coalescing results to one partition so it will create one csv file only
    weatherDataTransformed.foreach(element =>
      element._2
        .coalesce(1)
          .write
            .format("csv")
              .option("mode","OVERWRITE")
                .option("header","true")
                  .save(s"$path/${element._1}"))
  }
}
