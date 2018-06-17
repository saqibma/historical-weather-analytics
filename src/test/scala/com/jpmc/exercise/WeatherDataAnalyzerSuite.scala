package com.jpmc.exercise

/**
  * Created by adnan_saqib on 17/06/2018.
  */
class WeatherDataAnalyzerSuite extends BaseSuite {
  private var weatherDataAnalyzer: WeatherDataAnalyzer = _

  override def beforeAll() {
    super.beforeAll()
    weatherDataAnalyzer = new WeatherDataAnalyzer(spark)
  }

  test("Rank stations by long they have been online"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get the ranking")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected ranking data frames")
    val actualRanking = weatherDataTransformed.get("stationByCount").get
    val actualRankingWithColumnTypeCasted = actualRanking
      .withColumn("count", actualRanking("count").cast("String"))
    val expectedRanking = createDF("station-by-count.csv", "station,count")
    assertDataFrameEquals(actualRankingWithColumnTypeCasted, expectedRanking)
  }

  test("Rank stations by rainfall"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get the ranking")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected ranking data frames")
    val actualRanking = weatherDataTransformed.get("stationByRainfall").get
    val actualRankingWithColumnTypeCasted = actualRanking
      .withColumn("max(rainInMM)", actualRanking("max(rainInMM)").cast("String"))
    val expectedRanking = createDF("station-by-rainfall.csv", "station,max(rainInMM)")
    assertDataFrameEquals(actualRankingWithColumnTypeCasted, expectedRanking)
  }

  test("Rank stations by sunshine"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get the ranking")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected ranking data frames")
    val actualRanking = weatherDataTransformed.get("stationBySunshine").get
    val actualRankingWithColumnTypeCasted = actualRanking
      .withColumn("max(sunInHours)", actualRanking("max(sunInHours)").cast("String"))
    val expectedRanking = createDF("station-by-sunshine.csv", "station,max(sunInHours)")
    assertDataFrameEquals(actualRankingWithColumnTypeCasted, expectedRanking)
  }

  test("When was the worse rainfall for each station"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get worst rainfall year and month by Station")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected worst rainfall year and month by Station data frames")
    val actualWorstRainfallYearAndMonthByStation = weatherDataTransformed.get("worstRainfallYearAndMonthByStation").get
    val actualWorstRainfallYearAndMonthByStationColumnTypeCasted = actualWorstRainfallYearAndMonthByStation
      .withColumn("rainInMM", actualWorstRainfallYearAndMonthByStation("rainInMM").cast("String"))
    val expectedWorstRainfallYearAndMonthByStation = createDF("worst-rainfall-year-month-by-station.csv", "station,year,month,rainInMM")
    assertDataFrameEquals(actualWorstRainfallYearAndMonthByStationColumnTypeCasted, expectedWorstRainfallYearAndMonthByStation)
  }

  test("When was the best sunshine for each station"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get best sunshine year and month by Station")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected best sunshine year and month by Station data frames")
    val actualBestSunshineYearAndMonthByStation = weatherDataTransformed.get("bestSunshineYearAndMonthByStation").get
    val actualBestSunshineYearAndMonthByStationColumnTypeCasted = actualBestSunshineYearAndMonthByStation
      .withColumn("sunInHours", actualBestSunshineYearAndMonthByStation("sunInHours").cast("String"))
    val expectedBestSunshineYearAndMonthByStation = createDF("best-sunshine-year-and-month-by-station.csv", "station,year,month,sunInHours")
    assertDataFrameEquals(actualBestSunshineYearAndMonthByStationColumnTypeCasted, expectedBestSunshineYearAndMonthByStation)
  }

  test("Average rainfall for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Average rainfall for May across all stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Average rainfall for May across all stations data frames")
    val actualAverageRainfallForMayAcrossAllStations = weatherDataTransformed.get("averageRainfallForMayAcrossAllStations").get
    val actualAverageRainfallForMayAcrossAllStationsColumnTypeCasted = actualAverageRainfallForMayAcrossAllStations
      .withColumn("avg(rainInMM)", actualAverageRainfallForMayAcrossAllStations("avg(rainInMM)").cast("String"))
    val expectedAverageRainfallForMayAcrossAllStations = createDF("average-rainfall-for-may-across-all-stations.csv", "year,avg(rainInMM)")
    assertDataFrameEquals(actualAverageRainfallForMayAcrossAllStationsColumnTypeCasted, expectedAverageRainfallForMayAcrossAllStations)
  }

  test("Average sunshine for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Average sunshine for May across all stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Average sunshine for May across all stations data frames")
    val actualAverageSunshineForMayAcrossAllStations = weatherDataTransformed.get("averageSunshineForMayAcrossAllStations").get
    val actualAverageSunshineForMayAcrossAllStationsColumnTypeCasted = actualAverageSunshineForMayAcrossAllStations
      .withColumn("avg(sunInHours)", actualAverageSunshineForMayAcrossAllStations("avg(sunInHours)").cast("String"))
    val expectedAverageSunshineForMayAcrossAllStations = createDF("average-sunshine-for-may-across-all-stations.csv", "year,avg(sunInHours)")
    assertDataFrameEquals(actualAverageSunshineForMayAcrossAllStationsColumnTypeCasted, expectedAverageSunshineForMayAcrossAllStations)
  }

  test("Best year for Average Rainfall for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Best year for Average Rainfall for May across all Stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Best year for Average Rainfall for May across all stations data frames")
    val actualBestYearForAverageRainfallForMayAcrossAllStations = weatherDataTransformed.get("bestYearForAverageRainfallForMayAcrossAllStations").get
    val actualBestYearForAverageRainfallForMayAcrossAllStationsColumnTypeCasted = actualBestYearForAverageRainfallForMayAcrossAllStations
      .withColumn("Maximum Average Rain Across All Stations for May",
        actualBestYearForAverageRainfallForMayAcrossAllStations("Maximum Average Rain Across All Stations for May").cast("String"))
    val expectedBestYearForAverageRainfallForMayAcrossAllStations =
      createDF("best-year-for-average-rainfall-for-may-across-all-stations.csv", "Year,Maximum Average Rain Across All Stations for May")
    assertDataFrameEquals(actualBestYearForAverageRainfallForMayAcrossAllStationsColumnTypeCasted, expectedBestYearForAverageRainfallForMayAcrossAllStations)
  }

  test("Worst year for Average Rainfall for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Worst year for Average Rainfall for May across all Stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Worst year for Average Rainfall for May across all stations data frames")
    val actualWorstYearForAverageRainfallForMayAcrossAllStations = weatherDataTransformed.get("worstYearForAverageRainfallForMayAcrossAllStations").get
    val actualWorstYearForAverageRainfallForMayAcrossAllStationsColumnTypeCasted = actualWorstYearForAverageRainfallForMayAcrossAllStations
      .withColumn("Minimum Average Rain Across All Stations for May",
        actualWorstYearForAverageRainfallForMayAcrossAllStations("Minimum Average Rain Across All Stations for May").cast("String"))
    val expectedWorstYearForAverageRainfallForMayAcrossAllStations =
      createDF("worst-year-for-average-rainfall-for-may-across-all-stations.csv", "Year,Minimum Average Rain Across All Stations for May")
    assertDataFrameEquals(actualWorstYearForAverageRainfallForMayAcrossAllStationsColumnTypeCasted, expectedWorstYearForAverageRainfallForMayAcrossAllStations)
  }

  test("Best year for Average Sunshine for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Best year for Average Sunshine for May across all Stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Best year for Average Sunshine for May across all stations data frames")
    val actualBestYearForAverageSunshineForMayAcrossAllStations = weatherDataTransformed.get("bestYearForAverageSunshineForMayAcrossAllStations").get
    val actualBestYearForAverageSunshineForMayAcrossAllStationsColumnTypeCasted = actualBestYearForAverageSunshineForMayAcrossAllStations
      .withColumn("Maximum Average Sunshine Across All Stations for May",
        actualBestYearForAverageSunshineForMayAcrossAllStations("Maximum Average Sunshine Across All Stations for May").cast("String"))
    val expectedBestYearForAverageSunshineForMayAcrossAllStations =
      createDF("best-year-for-average-sunshine-for-may-across-all-stations.csv", "Year,Maximum Average Sunshine Across All Stations for May")
    assertDataFrameEquals(actualBestYearForAverageSunshineForMayAcrossAllStationsColumnTypeCasted, expectedBestYearForAverageSunshineForMayAcrossAllStations)
  }

  test("Worst year for Average Sunshine for May across all stations"){
    Given("list of stations")
    val stationListFile = "station-list.dat"

    When("Call WeatherDataAnalyzer extract and transform methods and get Worst year for Average Sunshine for May across all Stations")
    val weatherDataExtracted = weatherDataAnalyzer.extract(stationListFile)
    val weatherDataTransformed = weatherDataAnalyzer.transform(weatherDataExtracted)

    Then("Compare actual and expected Worst year for Average Sunshine for May across all stations data frames")
    val actualWorstYearForAverageSunshineForMayAcrossAllStations = weatherDataTransformed.get("worstYearForAverageSunshineForMayAcrossAllStations").get
    val actualWorstYearForAverageSunshineForMayAcrossAllStationsColumnTypeCasted = actualWorstYearForAverageSunshineForMayAcrossAllStations
      .withColumn("Minimum Average Sunshine Across All Stations for May",
        actualWorstYearForAverageSunshineForMayAcrossAllStations("Minimum Average Sunshine Across All Stations for May").cast("String"))
    val expectedWorstYearForAverageSunshineForMayAcrossAllStations =
      createDF("worst-year-for-average-sunshine-for-may-across-all-stations.csv", "Year,Minimum Average Sunshine Across All Stations for May")
    assertDataFrameEquals(actualWorstYearForAverageSunshineForMayAcrossAllStationsColumnTypeCasted, expectedWorstYearForAverageSunshineForMayAcrossAllStations)
  }

  override def afterAll() {
    spark.stop()
    // Clearing the driver port so that we don't try and bind to the same port on restart.
    System.clearProperty("spark.driver.port")
    super.afterAll()
  }
}
