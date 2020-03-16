import java.io.{PrintWriter, File, FileOutputStream}
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}

case class Pair(cat: String, value: Double)
case class Record(Category: String, Mean: Double, Variance: Double)

object SparkProject extends App{
    
  override def  main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Project").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    sc.setLogLevel("WARN")
    
    def mean(numbers: Array[Double]): Double = {
      if (numbers.length == 0) return 0.0
      numbers.sum.toDouble / numbers.length.toDouble  
    }
  
    def variance(numbers: Array[Double]): Double = {
      val avg = mean(numbers)
      if (numbers.length == 1) return 0.0
      numbers.map(x => (x - avg)*(x - avg)).sum.toDouble / (numbers.length - 1)
    }
  
    def computeMeanVarianceByCategory(df: DataFrame): DataFrame = {
      var dfOut = Seq.empty[Record].toDF()
      for (cat <- df.select("cat").distinct().map(_.getString(0)).collect().toArray){
        val arr = df.filter($"cat" === cat).select("value").map(_.getDouble(0)).collect().toArray
        val newRow = Seq(Record(cat.toString, mean(arr), variance(arr))).toDF()
        dfOut = dfOut.unionAll(newRow)
      }
      dfOut
    }
  
    def createResample(sample: DataFrame, iter: Int=10): DataFrame = {
      var dfSample = Seq.empty[Record].toDF()
      for (_ <- 1 to iter){
        val resample = sample.sample(true, 1.0)
        dfSample = dfSample.unionAll(computeMeanVarianceByCategory(resample))
      }
      dfSample
    }
  
    def computeAverage(df: DataFrame): DataFrame = {
      var dfOut = Seq.empty[Record].toDF()
      for (cat <- df.select("Category").distinct().map(_.getString(0)).collect().toArray){
        val arrMean = df.filter($"Category" === cat).select("Mean").map(_.getDouble(0)).collect().toArray
        val arrVariance = df.filter($"Category" === cat).select("Variance").map(_.getDouble(0)).collect().toArray
        val newRow = Seq(Record(cat.toString, mean(arrMean), mean(arrVariance))).toDF()
        dfOut = dfOut.unionAll(newRow)
      }
      dfOut
    }
  
    def absoluteError(df_real: DataFrame, df_est: DataFrame): Array[Double] = {
      val df1_cat = df_real.select("Category").map(r => r.getString(0)).collect.toList
      val df1_mean = df_real.select("Mean").map(r => r.getDouble(0)).collect.toList
      val df1_var = df_real.select("Variance").map(r => r.getDouble(0)).collect.toList
      
      val df2_cat = df_est.select("Category").map(r => r.getString(0)).collect.toList
      val df2_mean = df_est.select("Mean").map(r => r.getDouble(0)).collect.toList
      val df2_var = df_est.select("Variance").map(r => r.getDouble(0)).collect.toList
      
      val length = df1_cat.length
      val output = Array.fill[Double](length*2)(0)
      for (i <- 0 until length) {
        output(i*2) = Math.abs(df1_mean(i) - df2_mean(i))*100 /  df1_mean(i)
        output(i*2 +1) = Math.abs(df1_var(i) - df2_var(i))*100 /  df1_var(i)
      }
      output
    }
    
    if (args.length != 2){
       println("\n Wrong syntax: SparkProject <input file> <columns>")
       System.exit(1)
    }
    val input = args(0)
    val columns = args(1).split(",")

    // Step 1. Load dataset from .csv file
    val csv = sqlContext.read.format("com.databricks.spark.csv")
                        .option("header", "true").option("inferSchema", "true").load(input)
    println("=== Show data from .csv file ===")
    csv.show(5)
    
    // Step 2. Create a pair RDD called "population"
    val population = csv.selectExpr("cast(" + columns(0) + " as string) cat",
                                    "cast(" + columns(1) + " as double) value")
    
    // Step 3. Compute the mean mpg and variance for each category
    println("=== Mean and Variance by category (population) ===")
    val dfPop = computeMeanVarianceByCategory(population)
    dfPop.show()
    
    val file = new File("output.csv")
    val writer = new PrintWriter(file)
    var headers = dfPop.select("Category").distinct()
                       .map(r => r.getString(0) + " mean, " + r.getString(0) + " variance")
                       .collect.toList
    writer.write("ratio," + headers.mkString(",") + "\n")
    
    for (ratio <- 0.25 until 1.0 by 0.25){
      println(s"ratio = $ratio")
      // Step 4. Create the sample for bootstrapping.
      val sample = population.sample(false, ratio)
      // Step 5. Do 1000 times
      val iter = 10
      val dfSample = createResample(sample, iter)
      // Step 6. Get average of mean and variance
      val dfAvg = computeAverage(dfSample)
      println("=== Mean and Variance by category (average) ===")
      dfAvg.show()
      // Step 7. Determine the absolute error percentage
      var arr = ratio +: absoluteError(dfPop, dfAvg)
      writer.write(arr.mkString(",") + "\n")
    }
    
    // Step 8. Draw a graph with x-axis percentage
    writer.close()
    val out = sqlContext.read.format("com.databricks.spark.csv")
                        .option("header", "true").option("inferSchema", "true")
                        .load("file://" + file.getAbsolutePath())
    println("=== Absolute error percentage by ratio  ===")
    out.show
    
  }
}