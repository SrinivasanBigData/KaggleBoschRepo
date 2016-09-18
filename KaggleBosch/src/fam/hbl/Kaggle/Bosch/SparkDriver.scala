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
	def split_train_validation_data_fine (data:DataFrame, validation_percentage:Double):(DataFrame,DataFrame) = {
			// get the test data
			val validationDF= data.sample(false, validation_percentage)
					// get the training data as data that is not in test
					val trainDF= data.except(validationDF)
					// return both data frames
					return (trainDF, validationDF)
	}

	/**
	 *  This function splits the data set in validation and train data
	 *  
	 *  NOTE:  this function is rough in the sense that there is no guarantee that 
	 *  there is no guarantee that the train data is different than the validation data
	 *  As a consequence the validation evaluation will be a bit more optimistic than it should be
	 *  One should use a relatively low percentage so the risk of overlaps will be relatively small.
	 */
	def split_train_validation_data_rough (data:DataFrame, validation_percentage:Double):(DataFrame,DataFrame) = {
			// get the test data
			val validationDF= data.sample(false, validation_percentage)
					// get the training data as data that is not in test
					val trainDF= data.sample(false, 1-validation_percentage)
					// return both data frames
					return (trainDF, validationDF)
	}

	def split_train_test_data (data:DataFrame, test_percentage:Double, rought_strategy:Boolean=true):(DataFrame,DataFrame) = {
		// compute the traain and validation set depending on the strategy	
	  val (trainDF, validationDF)=
					if (rought_strategy) {
						split_train_validation_data_rough (data, test_percentage)
					} else {
						split_train_validation_data_fine (data, test_percentage)
					}
	  // return the two data frames
	  return (trainDF, validationDF)
	}

}

