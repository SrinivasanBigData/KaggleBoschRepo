package fam.hbl.KabbleBosch.test

import fam.hbl.Kaggle.Bosch.SparkDriver
import org.apache.spark.sql.Row

object ReadCSV_UseCase extends App {
  val l1= List()
  val l2= List()
  println("l1==l2",l1==l2)
  val ll1= List(23, 234,   2,   "a")
  val ll2= List(23, 234,   2,   "a")
  println("ll1==ll2",ll1==ll2)
  val r1= Row(23, 234,   2,   "a")
  val r2= Row(23, 234,   2,   "a")
  println("r1==r2",r1==r2)
	// create a spark session
	val test_session= SparkDriver.config_session();
	// some data to create a DataFrame
	val data= SparkDriver.load_data(test_session, 
			"C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\labelled_data1.csv",
			sep=";");
	data.show()
	// test first row
	val data_1= data.take(1)(0).toSeq
	val expect_data= Row(23,234,2,"a").toSeq
	println("data_1: "+data_1+" data_1.getClass: "+data_1.getClass)
	println("e_data: "+expect_data+" e_data.getClass: "+expect_data.getClass)
	println( "result: "+data_1.toString()+" "+
	    expect_data.toString()+" "+
	    (data_1 == expect_data).toString() )
}