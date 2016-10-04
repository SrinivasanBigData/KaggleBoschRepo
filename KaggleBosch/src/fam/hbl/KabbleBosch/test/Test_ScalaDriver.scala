package fam.hbl.KabbleBosch.stackOverflow_UseCase

import collection.mutable.Stack  // Here only for example .... To Be Removed
import org.apache.spark.sql.Row
import org.scalatest._

import fam.hbl.Kaggle.Bosch.SparkDriver

import java.io.File


class Test_ScalaDriver extends FlatSpec with Matchers with SparkDriver{
  
  /** --- Setting the context ------------------------------------
   *  To run the test it is required that there is a spark DataFrame that contains 
   *  some data.  
   */
	// create a spark session
	val test_session= config_session();
	// path of the test data frame
	val testDataDir= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\"
	val testDataName= "labelled_data1.csv"
	val testDataFrame= testDataDir+testDataName
	
  "SparkDriver_load" should "read a file correctly" in {
    // read the data
    val dataDF= load_data(test_session, testDataFrame, sep=";");
    // get first row of as a sequence
    val row1= dataDF.take(1)(0).toSeq
    // expected value of the first row
    val expect_data= Row("23","234","2","a").toSeq
    //check whether they are the same
    row1 should be (expect_data)
    dataDF.count() should be (9)
  }
  
  // test the saving function 
  def test_data_path= "C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\"
  def recordDF_path= test_data_path+"RecordData.csv"
  // read data frame that can be used for testing
  val dataDF= load_data(test_session, testDataFrame, sep=";").persist();
  // three cases to test
  // 1. the file did not exist, and it is saved
  // 2. the file exists,  replace with a new one
  // 3. the file exists but it cannot be overwritten
  
  // test case 1.  ----
  
  
  "SparkDriver_delete_recordDF" should "delete the record file when it exists" in {
    // check whether the file exists
    val recordFile:File = new File(recordDF_path)
    // check whether the file exists
    val fileExists:Boolean= recordFile.exists()
    // decide what to do
    val fileRemoved= if (fileExists) {
      // delete the file
       delete_recordDF(new File(recordDF_path))
       // the file should have been removed 
       assert( !recordFile.exists() )
       !recordFile.exists()
    } else {
      // assume that everything is fine
      succeed
      true
    }
    fileRemoved should be (true)
  }
  
  
  "SparkDriver_recordDF2File" should "save a new file correctly" in {
    // make sure that the file does not exist
    delete_recordDF(new File(recordDF_path))
    // record a data frame
    recordDF2File (dataDF, recordDF_path) 
    // re-load it
    val recordedDataDF= load_data(test_session, recordDF_path);
    // test whether the original DF and the recorded one are the same 
    val dataDF_row1= dataDF.take(1)(0).toSeq
    val recordedDF_row1= recordedDataDF.take(1)(0).toSeq
    // perform the tests
    recordedDF_row1 should be (dataDF_row1)
    recordedDataDF.count() should be (dataDF.count())
  }
  
  // not the file exists redo with overwrite
  it should "overwrite the file when requested" in {
    // record a data frame with overwrite
    recordDF2File (dataDF, recordDF_path,true) 
    // re-load it
    val recordedDataDF= load_data(test_session, recordDF_path);
    // test whether the original DF and the recorded one are the same 
    val dataDF_row1= dataDF.take(1)(0).toSeq
    val recordedDF_row1= recordedDataDF.take(1)(0).toSeq
    // perform the tests
    recordedDF_row1 should be (dataDF_row1)
    recordedDataDF.count() should be (dataDF.count())
  } 
  
  // test the transformation to labeled points
  
  
  // ---------------------------------------------------------------------

  "A Stack" should "pop values in last-in-first-out order" in {
    val stack = new Stack[Int]
    stack.push(1)
    stack.push(2)
    stack.pop() should be (2)
    stack.pop() should be (1)
  }

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val emptyStack = new Stack[Int]
    a [NoSuchElementException] should be thrownBy {
      emptyStack.pop()
    } 
  }
  
}