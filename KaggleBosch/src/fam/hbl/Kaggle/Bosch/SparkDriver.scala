package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame}

class SparkDriver  {}

object SparkDriver {

	def config_session (hadoop_dir:String, spark_warehouse_dir:String):SparkSession = {

			// set Hadoop reader to read files and configure dataframes
			System.setProperty("hadoop.home.dir", "C:\\Users\\Massimo\\Code\\hadoop-common-2.2.0-bin-master")
			println("Done Set Property!")

			// define the spark session
			val session = SparkSession  
			.builder()
			.appName("Spark SQL Example")
			//  .config("spark.some.config.option", "some-value")
			.master("local")
			.config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
			.getOrCreate()

			// For implicit conversions like converting RDDs to DataFrames
			import session.implicits._

			// return the session created
			return session
	}
  
	/**
	 *  This function splits the data set in test and train data
	 */
	def split_train_test_data_fine (data:DataFrame, test_percentage:Double):(DataFrame,DataFrame) = {
	  // get the test data
	  val test= data.sample(false, test_percentage)
	  // get the training data as data that is not in test
	  val train= data.except(test)
	  // return both data frames
	  return (train, test)
	}
  
	/**
	 *  This function splits the data set in test and train data
	 */
	def split_train_test_data_rough (data:DataFrame, test_percentage:Double):(DataFrame,DataFrame) = {
	  // get the test data
	  val test= data.sample(false, test_percentage)
	  // get the training data as data that is not in test
	  val train= data.sample(false, 1-test_percentage)
	  // return both data frames
	  return (train, test)
	}
	
	def split_train_test_data (data:DataFrame, test_percentage:Double, val rought_strategy=true):(DataFrame,DataFrame) = {
	  if (rought_strategy) {
	    split_train_test_data_rough (data, test_percentage)
	  } else {
	    split_train_test_data_fine (data, test_percentage)
	  }
	}
  
}