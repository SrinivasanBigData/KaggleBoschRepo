package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{Dataset,Row,DataFrame}

object BoschExploration {

	def show (data:Dataset[Row]) {
	  // sshow the data schema
		data.printSchema()
		// get a few random lines
		val data_sample= data.sample(false, 0.01)
		// print 5 of them
		data_sample.show(5)
	}
	
	/*
	 * There is a discussion on the categorical features in 
	 * https://www.kaggle.com/c/bosch-production-line-performance/forums/t/23135/questions-about-categorical-csv
	 * 
	 * My impression is the following
	 * I just started on this data set,  so I may say something obvious or just said,  but if the features are bianry representations,  then we can recognize hidden features.  For example
	 * 
	 * 20= 100110100
	 * 
	 * this may mean:
	 * 
	 * f1 f2 f3 f4 f5 f6 f7 f8 f9
	 *  1  0  0  1  1  0  1  0  0
	 * 
	 * Then it is not difficult to recognize the following:
	 * 
	 *       f9 f8 f7 f6 f5 f4 f3 f2 f1
	 *    T1  0  0  0  0  0  0  0  0  1
	 *    T4  0  0  0  0  0  0  1  0  0
	 *    T8  0  0  0  0  0  1  0  0  0
	 *    T9  0  0  0  0  0  1  0  0  1
	 *   T20  1  0  0  1  1  0  1  0  0
	 */
	
	def explore (data:DataFrame) {
	  // when showing data  it seems that most of the responses are 1
	  // one first question is:  What is the percentage of responses that are 1?
	 
	  // select the response feature
	  val response1= data.select("Id","Response").where("Response==1")
	  
	  response1.show(10)
	  
//	  val data_count:Double= data.count()
//	  val response1_count:Double= response1.count()
//	  val ratio:Double= (response1_count/data_count)*100
//	  
//	  println("data has "+data_count+" rows,"+
//	      " response1 has "+response1_count+"rows"+
//	      " The ratio is "+ratio+
//	      " the ratio between them is ")
	}

}