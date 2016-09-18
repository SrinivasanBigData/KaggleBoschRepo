package fam.hbl.Kaggle.Bosch

/* 
 * Log4j Configuration:
 * Run->Run Configuration -> [classpath tab] -> click on user Entries -> Advanced -> 
 * Select Add Folder -> select the location of your log4j.properties file
 * 
 */

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame}
import org.apache.log4j.PropertyConfigurator
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object BoschDriver extends App {
  
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
	
	val data= reader.option("header", "true") .csv(path)

	println("Completed Reading")
	
	val (train,test)= SparkDriver.split_train_test_data(data,.2)

	println("Split train test")
	
	// start data exploration
	
	BoschExploration.explore(train)
	
	
	
	//data
	
	


	println("Done!")


}