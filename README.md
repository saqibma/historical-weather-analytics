Spark Based Historical Weather Analytics App

Please go through the following points before running the application.
1) Station list can be found in station-list.dat file kept under resources directory.
2) Please run the main program MainApp.scala in order to perform historical weather analytics.
3) Please provide full path to the output folder as an argument to the main program.
   For example C:\dev\result
4) Please make sure to delete output files and folders produced before running the main program second time.
5) WeatherDataAnalyzer performs extraction, transformation and load activities.
   Please refer WeatherDataAnalyzer.scala class.
6) WeatherDataAnalyzer extract method parses and extracts historical weather data from the met office site.
7) WeatherRestClient consumes station data from the met office site.
8) WeatherDataAnalyzer transform method performs all the transformations asked in the exercise.
9) WeatherDataAnalyzer load method saves transformed dataframes as csv files(with header) under the output
   folder specified as an argument to the main program.
10) Integration testing was done in a BDD style for each transformations asked in the exercise.
    Please refer WeatherDataAnalyzerSuite.scala class.
11) Please run WeatherDataAnalyzerSuite.scala class in order to test all the transformations.
12) Transformed dataframes are being validated against the pre-calculated results present in csv files under
    src\test\resources directory as a part of the integration test
13) A generic spark based test framework was designed and developed to test all kinds of RDDs and data frames.
Please refer BaseSuite.scala class for the detail implementation.
14) pom.xml file was updated to create a jar with dependencies called uber jar.

