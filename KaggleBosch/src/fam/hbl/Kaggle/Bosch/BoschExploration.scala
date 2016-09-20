package fam.hbl.Kaggle.Bosch

import org.apache.spark.sql.{Dataset,Row,DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

object BoschExploration {
	
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
  
  def check_response_1_cases (dataDF:DataFrame) = {
    
	  // when showing data  it seems that most of the responses are 0
	  // one first question is:  What is the percentage of responses that are 1?
	 
	  // select the response feature
	  val response1DF= dataDF.select("Id","Response").where("Response==1")
	  
	  response1DF.show(10)
  }
  
//  def dataframe2labelledPoints (dataDF:DataFrame): RDD[LabeledPoint]{
//    TBD
//  }
//  
  
  /**
   * 
 * @param dataDF
 * @param numTopFeatures
 * @return
 */
def score_features_relevance (dataDF:DataFrame,numTopFeatures:Int=100) :RDD[LabeledPoint] = {
    // transform the data frame in an RDD[labeledPoints] to be able to use it for ML
   val lab_pts= SparkDriver.df2LabeledPoints(dataDF, "Response")
   // extract the mnost relevant features
   return SparkDriver.feature_selection (lab_pts, numTopFeatures) 
  }
	
  
	/**  
	 * Exploration driver
	 * @param dataDF:  the data to explore
	 */
	def explore (dataDF:DataFrame) {
	  // check on the 1 cases
	  check_response_1_cases (dataDF)
	  //
	  score_features_relevance (dataDF, 100)
	}
	
}