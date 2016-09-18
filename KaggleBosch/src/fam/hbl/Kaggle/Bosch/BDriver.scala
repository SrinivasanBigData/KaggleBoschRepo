package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.DataFrame

object BDriver extends App {
  
  /* 
   * ------ SETTINGS --------------------------------------------
   */
	 
  val hadoop_dir= "C:\\Users\\Massimo\\Code\\hadoop-common-2.2.0-bin-master"

  val spark_warehouse_dir= "file:///tmp/spark-warehouse"

	println("Start!")

	// get a spark session
	val session= SparkDriver.config_session(hadoop_dir, spark_warehouse_dir)

	println("Done Spark Session")

	val reader= session.read

	println("Done Spark Reader")

	// read data file in Spark

	val directoryName= "\\Users\\Massimo\\Code\\Data\\Kaggle\\Bosch\\"
	
	val fileName= "train_numeric.csv"
	
	val path= directoryName+fileName
	
	val allDataDF= reader.option("header", "true") .csv(path)
	
	val dataDF:DataFrame= allDataDF.sample(false, 0.02)
	
	println("dataDF.count(): "+dataDF.count())

	println("Completed Reading")
	
	// val (train,test)= SparkDriver.split_train_test_data(data,.2)

  // get the test data
	val testDF= dataDF.sample(false, 0.2)
	
	testDF.show(2)
	
	// get the training data as data that is not in test
	val trainDF= dataDF.except(testDF)
	
	trainDF.show(2)
	
	// start data exploration
	
	// BoschExploration.explore(train)
	
	println("Done!")
}

