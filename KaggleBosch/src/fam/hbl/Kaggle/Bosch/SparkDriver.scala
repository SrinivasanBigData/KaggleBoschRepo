package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame,DataFrameWriter}
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors,DenseVector}

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
	 *  Record a DataFrame to file to avoid to rebuild everything on startup
	 */
	def recordDF2File (dataDF:DataFrame, dataDF_path:String) = {
	  // create a dataframe writer
	  val writer= dataDF.write
	  // store the file as parquet
	  writer.option("header", true).csv(dataDF_path)
	}

	
	/** 
	 *  extracts validation and train data.
	 *  Split train data in train and rest in case only part of train data to be used.  
	 */
	def split_train_validation_data_fine (dataDF:DataFrame, validation_ratio:Double, ignore_ratio:Double=1):(DataFrame,DataFrame) = {
			// compute the train ratio
			val train_ratio= 1-(validation_ratio+ignore_ratio)
					// split the data
					val splits = dataDF.randomSplit(Array(train_ratio, validation_ratio, ignore_ratio))
					// name the ratios
					val (trainDF, validationDF) = ( splits(0), splits(1) )
					// return statement
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
	def split_train_validation_data_rough (data:DataFrame, validation_ratio:Double, ignore_ratio:Double=0):(DataFrame,DataFrame) = {
			// get the test data
			val validationDF= data.sample(false, validation_ratio)
					// get the training data as data that is not in test
					val trainDF= data.sample(false, 1-(validation_ratio+ignore_ratio))
					// return both data frames
					return (trainDF, validationDF)
	}

	def split_train_validation_data (data:DataFrame, validation_ratio:Double, ignore_ratio:Double=0, rought_strategy:Boolean=true):(DataFrame,DataFrame) = {
			// compute the traain and validation set depending on the strategy	
			val (trainDF, validationDF)=
					if (rought_strategy) {
						split_train_validation_data_rough (data, validation_ratio)
					} else {
						split_train_validation_data_fine (data, validation_ratio)
					}
			// return the two data frames
			return (trainDF, validationDF)
	}

	/*
	 * used to reduce the size of the training set
	 * - faster computation
	 * - higher abstraction
	 */
	def reduce_train (trainDF:DataFrame, train_sample_ratio:Double=1): DataFrame= {
			return trainDF.sample(false, train_sample_ratio)
	}


	// -------------------------------------------

	def row2LabeledPoint (row:Row, target_ind:Int,features_indexes:Array[Int]) : LabeledPoint= {
			// extract the label from the row
			val label:Double= row.getDouble(target_ind)
					// extract the features
					val features_vals:Array[Double]= features_indexes.map(row.getDouble(_))
					val features:Vector = Vectors.dense(features_vals).toSparse
					// build the labeledPoint and return it
					return(LabeledPoint(label,features))
	}

	def df2LabeledPoints (df:DataFrame, target:String) : RDD[LabeledPoint] = {
			// Step 1: get the index of the independent variable
			val target_ind= df.columns.indexOf(target)
					// Step 2: get indexes of the independent variables
					// - get all columns, and remove the target column
					val features:Array[String]= df.columns.diff(target)
					// - extract indexes
					val features_indexes:Array[Int]= features.map(df.columns.indexOf(_))
					// Step 3. map the rows of the DF to labeled points
					val rdd_labeledPoint= df.rdd.map( row => row2LabeledPoint(row,target_ind,features_indexes) )
					// return the rdd of labeled points
					return(rdd_labeledPoint)
	}

	// run feature selection

	/**
	 * code from: https://spark.apache.org/docs/latest/mllib-feature-extraction.html#standardscaler
	 */
	def feature_selection (dataRDD:RDD[LabeledPoint], numTopFeatures:Int):RDD[LabeledPoint] = {
		// Create ChiSqSelector that will select top 50 of 692 features
		val selector:ChiSqSelector = new ChiSqSelector(50)
				// Create ChiSqSelector model (selecting features)
				val transformer:ChiSqSelectorModel = selector.fit(dataRDD)
				// Filter the top 50 features from each feature vector
				val filteredData:RDD[LabeledPoint] = dataRDD.map { lp => LabeledPoint(lp.label, transformer.transform(lp.features)) }
		  return(filteredData)
	}
}
