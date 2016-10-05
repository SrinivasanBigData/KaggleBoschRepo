package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{SparkSession,Dataset,Row,DataFrame}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.mllib.util.MLUtils

import org.apache.log4j.Logger
import org.apache.log4j.Level.DEBUG

object SampleExploration extends App with SparkDriver {

	/* 
	 * ------ Logging  --------------------------------------------
	 */
	val expl_logger = Logger.getLogger("SampleExploration");
	//
	expl_logger.setLevel(DEBUG)

	/* 
	 * ------ SETTINGS --------------------------------------------
	 */

	// got a spark session from SparkDriver

	expl_logger.debug("SampleExploration: got session")

	// get the data 
	val dataDir= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\"
	val dataFileName= "ColumnSelectionTestData.csv"
	val dataPath= dataDir+dataFileName

	expl_logger.debug("SampleExploration: got directories")

	// load data
	val (train,validation)= 
	getData_split (
			dataPath,
			dataDir+"ColumnSelectionTestData_train", 
			dataDir+"ColumnSelectionTestData_validation", 
			session)

	expl_logger.debug("SampleExploration: got loaded data")

	// ------------------------  Transform the data into labelled points
	/**
	 * THIS FUNCTION SHOULD BE IN SparkDriver BUT THERE THERE IS A PROBLEM WITH 
	 * CLOSURE OS IT IS TEMPORARILY HERE
	 * Given a DataFrame and a response column, 
	 * return the same dataframe formatted as labelled points 
	 * where response is the label column and the other columns are the features
	 * 
	 * @param df:the dataframe
	 * @param respnse the column 
	 * @return a dataset with two columns label and features
	 */
	def df2LabeledPoints (df:DataFrame, response:String) : Dataset[LabeledPoint] = { 
			// Separate response from features
			val features:Array[String]= df.columns.diff(Array(response))
					// get indexes
					val response_index:Int= df.columns.indexOf(response);
			val features_indexes:Array[Int]= features.map(df.columns.indexOf(_));
			// 
			import session.implicits._
			val labeledPoints= df.map({ row:Row => row2LabeledPoint(row,response_index,features_indexes) } );
			labeledPoints.show(false) // Needed only for debugginh
			return(labeledPoints)
	}
	// define transform the train data into labelled points
	val trainLP:Dataset[LabeledPoint]= df2LabeledPoints(train, "Response").persist();
	expl_logger.debug("SampleExploration: got LabeledPoints")
	
	// save training trainLP as backup
	//MLUtils.saveAsLibSVMFile(trainLP.toJavaRDD, dataDir+"trainLP")
	
	// ===================================== ML Training =========================
	
	//val trainRDD= MLUtils.loadLibSVMFile(session.sparkContext, dataDir+"trainLP")
	// scaling the data
	val scaler:StandardScaler= new StandardScaler("features","features",false,false)
	//...was... val fit:StandardScalerModel= scaler.fit(trainLP)
	import session.implicits._
	val fit:StandardScalerModel= scaler.fit(trainLP)
	//val scalerModel = new StandardScalerModel(false,false)
	val tranform= fit.transform(trainLP)
	
	tranform.show(true)
	// identify relevant features

}
