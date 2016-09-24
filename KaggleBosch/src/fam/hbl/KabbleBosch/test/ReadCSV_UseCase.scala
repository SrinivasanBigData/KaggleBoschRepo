package fam.hbl.KabbleBosch.test

import fam.hbl.Kaggle.Bosch.SparkDriver
import org.apache.spark.sql.Row

object ReadCSV_UseCase extends App with SparkDriver {
	// create a spark session
	val test_session= config_session();
	// path where to find the test data
	def data_path= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\"
	// some data to create a DataFrame
	val dataDF= load_data(test_session, 
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
	val recordedDataDF= load_data(test_session, data_path+"RecordData.csv");
	recordedDataDF.show()
	val recordedRow1= recordedDataDF.take(1)(0).toSeq
	sparkDriver_logger.debug("ReadCSV_found: "+recordedRow1+" expected: "+row1+" they are the same: "+(row1 == recordedRow1) );
}