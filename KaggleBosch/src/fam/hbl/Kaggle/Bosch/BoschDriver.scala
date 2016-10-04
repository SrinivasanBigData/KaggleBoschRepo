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

	// get a spark session from SparkDriver

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
	def getData (dataDir:String, dataFile:String, session:SparkSession): (DataFrame,DataFrame) = {
			// check whether the train and validation fines already exist.
			val trainPath= DefaultFileStore + DefaultTrainFile;
			val validationPath= DefaultFileStore + DefaultValidationFile;
			val dataPath= dataDir+dataFile
					// return the result of loading and splitting the data
					return(
							getData_split (dataPath, trainPath, validationPath, session)
							)
	}

	val (trainDF,validationDF)= 
			getData (
					"\\Users\\Massimo\\Code\\Data\\Kaggle\\Bosch\\",
					"train_numeric.csv",
					session:SparkSession);

	bd_logger.info("Completed Reading")

	// debug
	val train_count= trainDF.count()
	val validation_count= validationDF.count()
	bd_logger.info("Split train test: train_count= "+train_count+" validation_count= "+validation_count)

	// start data exploration
//	BoschExploration.explore(trainDF)


	bd_logger.info("Done!")
}