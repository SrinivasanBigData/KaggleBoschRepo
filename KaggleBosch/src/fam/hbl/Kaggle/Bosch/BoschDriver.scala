package fam.hbl.Kaggle.Bosch

/* 
 * Log4j Configuration:
 * Run->Run Configuration -> [classpath tab] -> click on user Entries -> Advanced -> 
 * Select Add Folder -> select the location of your log4j.properties file
 * 
 */

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame}

import org.apache.log4j.Logger
import org.apache.log4j.Level.DEBUG

import java.nio.file.{Paths, Files}

object BoschDriver extends App with SparkDriver{

	/* 
	 * ------ Logging  --------------------------------------------
	 */
	val bd_logger = Logger.getLogger("BoschApp");
	//
	bd_logger.setLevel(DEBUG)

	/* 
	 * ------ SETTINGS --------------------------------------------
	 */

	// required to create a session
	//	val hadoop_dir= "C:\\Users\\Massimo\\Code\\hadoop-common-2.2.0-bin-master";
	//
	//	val spark_warehouse_dir= "file:///tmp/spark-warehouse";

	bd_logger.info("Start! Bosch Driver");

	// get a spark session
	// val session= SparkDriver.config_session(hadoop_dir, spark_warehouse_dir);
	val session= config_session();

	bd_logger.info("Done Spark Session");


	/** read data in spark
	 *  session:  the spark session to use for loading
	 */
	def load_data (session:SparkSession):DataFrame = {
			// get directory
			val directoryName= "\\Users\\Massimo\\Code\\Data\\Kaggle\\Bosch\\";
			// get file to load
			val fileName= "train_numeric.csv";
			// form the path
			val path= directoryName+fileName;
			// load data and return
			return (load_data(session,path))
	} 

	/** 
	 *  location where to store the file 
	 */
	val DefaultFileStore= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\DF2File\\";
	/**
	 * default name of the training data file
	 */
	val DefaultTrainFile= "BoschTraining";
	/**
	 * default name validation file
	 */
	val DefaultValidationFile= "BoschValidation";

	/** Get the data for the session
	 *  If the data is 
	 *  
	 */
	def getData (session:SparkSession): (DataFrame,DataFrame) = {
			// check whether the train and validation fines already exist.
			val trainPath= DefaultFileStore + DefaultTrainFile;
			val validationPath= DefaultFileStore + DefaultValidationFile;
			val stored_data_exists= Files.exists(Paths.get(trainPath)) && Files.exists(Paths.get(validationPath));
			bd_logger.debug("BoschDriver.getData: ready to choose ")
			// make the choice between reading the data and build the validation and train data
			// or loading the data.  
			// the criteria is:  if the stored data exists,  then load it.
			//
			// create two empty variables for the data
			val (trainDF:DataFrame, validationDF:DataFrame) = 
			if (stored_data_exists) {
				bd_logger.debug("BoschDriver.getData: If chosen... files exist")
				// read the stored data
				val trainDF= session.read.option("header", "true").csv(trainPath); //...was... parquet(trainPath);
				val validationDF= session.read.option("header", "true").csv(validationPath); //...was... .parquet(validationPath);
				bd_logger.debug("BoschDriver.getData: done")
				( trainDF.persist(), validationDF.persist() );
			} else {
				bd_logger.debug("BoschDriver.getData: else chosen... files do not exist")
				// read the row data
				val dataDF= load_data(session);
				// split train and validation data
				val (trainDF,validationDF)= split_train_validation_data(dataDF,.2, .1, true);
				// store train and validation data
				bd_logger.debug("BoschDriver.getData: ready to store");
				recordDF2File(trainDF, trainPath);
				recordDF2File(validationDF, validationPath);
				// return valude
				( trainDF.persist(), validationDF.persist() );
			}
			// return the data read
			return (trainDF, validationDF)
	}

	val (trainDF,validationDF)= getData (session:SparkSession)

			bd_logger.info("Completed Reading")

			// debug
			val train_count= trainDF.count()
			val validation_count= validationDF.count()
			bd_logger.info("Split train test: train_count= "+train_count+" validation_count= "+validation_count)

			// start data exploration
			BoschExploration.explore(trainDF)


			bd_logger.info("Done!")
}