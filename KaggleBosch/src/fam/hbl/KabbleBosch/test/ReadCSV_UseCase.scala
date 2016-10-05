package fam.hbl.KabbleBosch.stackOverflow_UseCase

import fam.hbl.Kaggle.Bosch.SparkDriver
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.regression.LabeledPoint

object ReadCSV_UseCase extends App with SparkDriver {
	// path where to find the test data
	def data_path= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\"
			// some data to create a DataFrame
			val dataDF= load_data(session, 
					data_path+"labelled_data1.csv",
					sep=";").persist();
	dataDF.show()
	// test first row
	val row1= dataDF.take(1)(0).toSeq
	val expect_data= Row("23","234","2","a").toSeq
	sparkDriver_logger.debug( "result: "+row1.toString()+" "+
			expect_data.toString()+" "+
			(row1 == expect_data) );
	//
	sparkDriver_logger.debug("ReadCSV_data.count() "+dataDF.count())

	// try a test of the storing to file
	sparkDriver_logger.debug("ReadCSV_UseCase: Going to save the file")
	recordDF2File (dataDF, data_path+"RecordData.csv") 
	val recordedDataDF= load_data(session, data_path+"RecordData.csv");
	recordedDataDF.show()
	val recordedRow1= recordedDataDF.take(1)(0).toSeq
	sparkDriver_logger.debug("ReadCSV_found: "+recordedRow1+" expected: "+row1+" they are the same: "+(row1 == recordedRow1) );

	// ------------------------ DataFrame Remove a column
	val numDF= dataDF.drop("val4")
			numDF.show()

			// For implicit conversions from RDDs to DataFrames
			import session.implicits._

			// apply map to numDF
			def mapDF= numDF.map { row => row.length }
	mapDF.show

	// transform numDF to labeled Points
	val target_ind= 0
	val features_ind:Array[Int]= Array(1,2)

	// -------------- protect transformation
	// select the target column
	val protNumDF= numDF.filter { row => row.getString(target_ind) != "" }
	protNumDF.show()

	val labPointDF= protNumDF.map { row => row2LabeledPoint(row, target_ind,features_ind) }
	labPointDF.show()

	//	val labPointDF2= df2LabeledPoints (numDF, "val1")
	//	labPointDF2.show()

	val df= protNumDF
	val response= "val1"

	def df2lp (df:DataFrame, response:String) : Dataset[LabeledPoint] = { 
			// Separate response from features
			val features:Array[String]= df.columns.diff(Array(response));
	// get indexes
	val response_index:Int= df.columns.indexOf(response);
	val featres_indexes:Array[Int]= features.map(df.columns.indexOf(_));
	// protect df
	val protectedDF:DataFrame= df.filter { row => row.getString(target_ind) != "" }
	val labPointDS= protectedDF.map { row => row2LabeledPoint(row, target_ind,features_ind) };
	//labPointDS.show()

	return(labPointDS)
	}

	val labPointDS= df2lp (numDF, response)
			labPointDS.show()

	// CARE: This bombs
	val labPointDF2= df2LabeledPoints_2 (protNumDF, "val1")
	labPointDF2.show()


			// ------------------------ Go through all rows in DF
			// transform the data in RDD
			val dataRDD= dataDF.rdd

			// ---- map example
			val sc= session.sparkContext
			val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
			val b = a.map(_.length)
			val c = a.zip(b)
			println(c.collect)
}