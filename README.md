# cs522
CS522 Spark Project

spark-submit --jars libs/spark-csv_2.11-1.5.0.jar,libs/commons-csv-1.3.jar --class "SparkProject" --master local[4] target/scala-2.10/spark-project_2.10-1.0.jar /user/root/diamonds.csv color,price
