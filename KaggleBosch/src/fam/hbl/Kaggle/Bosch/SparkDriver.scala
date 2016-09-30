package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame,DataFrameWriter}
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.feature.ChiSqSelectorModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{Vector,Vectors,DenseVector}

import org.apache.log4j.Logger
import org.apache.log4j.Level.DEBUG

import java.io.{IOException,File}
import java.nio.file.{NoSuchFileException,DirectoryNotEmptyException}

//class SparkDriver  {}

trait SparkDriver {

	/* 
	 * ------ Logging  --------------------------------------------
	 */
	val sparkDriver_logger = Logger.getLogger("BoschApp");
	//
	sparkDriver_logger.setLevel(DEBUG)

	/* 
	 * ------ Logics --------------------------------------------
	 */

	// standard Hadoop dir
	val std_hadoop_dir= "C:\\Users\\Massimo\\Code\\hadoop-common-2.2.0-bin-master";
	// standard spark warehouse
	val std_spark_warehouse_dir= "file:///tmp/spark-warehouse";

	def config_session (hadoop_dir:String=std_hadoop_dir, 
			spark_warehouse_dir:String=std_spark_warehouse_dir):SparkSession = {

					// set Hadoop reader to read files and configure dataframes
					System.setProperty("hadoop.home.dir", "C:\\Users\\Massimo\\Code\\hadoop-common-2.2.0-bin-master")
					sparkDriver_logger.info("Done Set Property!")

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

	// ----------- Reading - Writing data ------------------------------

	/** read data in spark
	 *  @param session:  the spark session to use for loading
	 *  @paran path  the whole file path name including file name
	 *  @param header whether the file has an header or not
	 *  @return a data frame with the data in the file
	 */
	def load_data (session:SparkSession, path:String, sep:String=",", header:Boolean=true):DataFrame = {
			// establish whether to use a header
			val header_label= if (header) {"true"} else {"false"};
			// create the reader
			val reader= session.read;
			// read using csv format
			val dataDF= reader.option("header",header_label).option("sep", sep).csv(path);
			// return the data frame 
			return(dataDF) 
	}


	/** spark saves DataFrames in directories,  
	 *  these directories need to be cleaned before they are removed
	 *  @param file: the file to delete
	 *  @return true if the file has been deleted
	 *  @throw SecurityException when the delete function fails
	 */
	def delete_recordDF (file:File):Boolean = {
			sparkDriver_logger.debug("SparkDriver.recordDF2File: deleting file: "+file.getName);
			// check whether the file to delete is a directory
			val deleted= if (file.isDirectory()) {
				// get list of files in directory
				val files = file.listFiles();
				// recursively delete all files
				files.foreach { file => delete_recordDF(file) }
				// delete the (hopefully) empty directory
				file.delete()
			} else {
				// it is just a file,  not a directors, delete!
				file.delete()
			}
			// if nothing bombed return true
			return deleted
	}


	/**
	 *  Record a DataFrame to file to avoid to rebuild everything on startup
	 *  
	 */
	def recordDF2File (dataDF:DataFrame, dataDF_path:String, overwrite:Boolean=true) = {
			// ideally we could simply save the file but  Spark gives an exception if the file exists already, 
			// so the saving function is abstracted, and a protection is added to avoid the exception
			//
			// the core work of this function ----------
			def saveDF(dataDF:DataFrame, dataDF_path:String) = {
					sparkDriver_logger.debug("SparkDriver.recordDF2File.save: entering")
					// create a dataframe writer
					val writer= dataDF.write
					// store the file as parquet
					writer.option("header", true).csv(dataDF_path)
					sparkDriver_logger.debug("SparkDriver.recordDF2File.save: exiting")
			}
			//
			// The protection ---------------------------
			// check whether the file exists
			val file = new File(dataDF_path);
			// Spark gives an exception if the file exists already,  so we need to address the issue
			// if the file does not exist simply save
			// if the file exists and it can be overwritten: remove the file and then save
			// if the file exists and it cannot be overwritten,  let it be and simply exit
			if (file.exists()) {
				sparkDriver_logger.debug("SparkDriver.recordDF2File: File exists")
				// the file exist, check whether it can be overwritten
				if (overwrite) {
					sparkDriver_logger.debug("SparkDriver.recordDF2File: File overwrite")
					// the file exists,  and it can be overwritten
					// remove the old file
					val deleted= delete_recordDF(file)
					sparkDriver_logger.debug("SparkDriver.recordDF2File: File deleted: "+deleted)
					// save it again
					saveDF(dataDF, dataDF_path)
					sparkDriver_logger.debug("SparkDriver.recordDF2File: new file saved: ")
				} 
				else {
					sparkDriver_logger.debug("SparkDriver.recordDF2File: File exists and it is not overwritten")
					// the file exists and it cannot overwritten
					// --- do nothing
				}
			} 
			else {
				sparkDriver_logger.debug("SparkDriver.recordDF2File: File does not exist save it")
				// the file does not exist,  it can be safely saved
				saveDF(dataDF, dataDF_path)
			}	
	}


	/** 
	 *  extracts validation and train data.
	 *  Split train data in train and rest in case only part of train data to be used.  
	 */
	def split_train_validation_data_fine (dataDF:DataFrame, validation_ratio:Double, ignore_ratio:Double=1):(DataFrame,DataFrame) = {
			// compute the train ratio
			val train_ratio= 1-(validation_ratio+ignore_ratio);
			// split the data
			val splits = dataDF.randomSplit(Array(train_ratio, validation_ratio, ignore_ratio));
			// name the ratios
			val (trainDF, validationDF) = ( splits(0), splits(1) );
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

	/**
	 * This function is defined exclusively to debug a problem with the map 
	 * in the function row2labeledPoint --- No other scope.
	 */
	def debug_row2LabeledPoint(row:Row, index:Int):Double = {
			// get row value at index
			val value:AnyRef= row.getAs(index)
					// check whether the value is a string
					value match {
					case _: String => 
					sparkDriver_logger.debug("debug_row2LabeledPoint: found string ...>"+value+"<...")
					case _: Any => 
					sparkDriver_logger.trace("debug_row2LabeledPoint: found Any ...>"+value+"<...")
	}
	return row.getDouble(index)
	}

	/** Transform one row in a DataFrame into LabeledPoints to be used by machine learning algorithms
	 *  @author Massimo
	 *  @param row the row to transform
	 *  @param target_ind the index of the target column
	 *  @param feature_indexes The indexes of the features in the row
	 *  @return a labeled point for the row
	 */
	def row2LabeledPoint (row:Row, target_ind:Int,features_indexes:Array[Int]) : LabeledPoint= {
			// extract the label from the row
			val label:Double= row.getDouble(target_ind);
	sparkDriver_logger.debug("row2LabeldPoint: label== "+label);
	// extract the features
	val features_vals:Array[Double]= features_indexes.map(debug_row2LabeledPoint(row, _));
	val features:Vector = Vectors.dense(features_vals).toSparse;
	// build the labeledPoint and return it
	return(LabeledPoint(label,features))
	}

	def df2LabeledPoints (df:DataFrame, target:String) : RDD[LabeledPoint] = {
			// Step 1: get the index of the independent variable
			val target_ind= df.columns.indexOf(target);
			// Step 2: get indexes of the independent variables
			// - get all columns, and remove the target column
			val features:Array[String]= df.columns.diff(target);
			// - extract indexes
			val features_indexes:Array[Int]= features.map(df.columns.indexOf(_));
			// Step 3. map the rows of the DF to labeled points
			val rdd_labeledPoint= df.rdd.map( row => row2LabeledPoint(row,target_ind,features_indexes) );
			// return the rdd of labeled points
			return(rdd_labeledPoint)
	}

	// run feature selection

	/**
	 * code from: https://spark.apache.org/docs/latest/mllib-feature-extraction.html#standardscaler
	 */
	def feature_selection (dataRDD:RDD[LabeledPoint], numTopFeatures:Int):RDD[LabeledPoint] = {
			// Create ChiSqSelector that will select top 50 of 692 features
			val selector:ChiSqSelector = new ChiSqSelector(50);
	// Create ChiSqSelector model (selecting features)
	val transformer:ChiSqSelectorModel = selector.fit(dataRDD);
	// Filter the top 50 features from each feature vector
	val filteredData:RDD[LabeledPoint] = dataRDD.map { lp => LabeledPoint(lp.label, transformer.transform(lp.features)) };
	return(filteredData)
	}
}
