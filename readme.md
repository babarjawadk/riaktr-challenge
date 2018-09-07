# Riaktr Challenge

## Introduction
This is a spark application that computes a set of metrics based on a collection of CDRS. The application has been built locally (on an apple machine) using standalone spark. The application consumes 2 CSV files (`cdrs.csv` and `cells.csv`) and outputs 1 CSV file (`results.csv`).

## How to build and run the application
Since the application depends on the Spark API, there is an [sbt](https://www.scala-sbt.org/) condiguration file which explains that Spark is a dependency (please make sure to match it with your local installation of spark). 

For sbt to work correctly, we'll need to layout `RiaktrChallenge.scala` and `build.sbt` according to the typical directory structure. Once this is in place, we can create a JAR package containing the application's code, then use spark-submit to run the program.

Here are the detailed steps using bash:
1. `cd <path to project containing build.sbt and src/main/scala/RiaktrChallenge.scala>`
2. `sbt package`

You can find more detailed info on self-contained applications [here](http://people.apache.org/~pwendell/spark-nightly/spark-master-docs/latest/quick-start.html#self-contained-applications) and on how to install spark locally [here](https://medium.com/luckspark/installing-spark-2-3-0-on-macos-high-sierra-276a127b8b85).

## How to run the application
Once we have the JAR file, we can run the application as follows:
```
spark-submit --class "RiaktrChallenge" --master local[4] target/scala-2.11/riaktr-challenge_2.11-1.0.jar <path to cdr data> <path to cell data> <path to the results>
```
Note that you must match the `riaktr-challenge_2.11-1.0.jar` with the `.jar` you have built locally. The `.jar` is followed by 3 parameters that the application takes as input:
1. path to cdr data without the name of the file (example on mac: `/Users/userName/RiaktrChallenge`)
2. path to the cell data without the name of the file (example on mac: `/Users/userName/RiaktrChallenge`)
3. path to where we want the output csv to be stored without the name of the file (example on mac: `/Users/userName/RiaktrChallenge`)

Please also make sure to not include a slash (/) at the end of the 3 paths.

Finally note that the input files must be named cdrs.csv and cells.csv and the output file will be named results.csv.

## Assumptions on the input data
We assume that the data is clean, doesn't contain any duplicates and is in the same format as the sample provided through e-mail.
