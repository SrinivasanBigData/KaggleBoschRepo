package fam.hbl.KabbleBosch.test

import collection.mutable.Stack  // Here only for example .... To Be Removed
import org.apache.spark.sql.Row
import org.scalatest._

import fam.hbl.Kaggle.Bosch.SparkDriver


class Test_ScalaDriver extends FlatSpec with Matchers {
  
  /** --- Setting the context ------------------------------------
   *  To run the test it is required that there is a spark DataFrame that contains 
   *  some data.  
   */
	// create a spark session
	val test_session= SparkDriver.config_session();

  "Reading a file" should "return the correct values in the file" in {
    val data= SparkDriver.load_data(test_session, 
			"C:\\Users\\Massimo\\Code\\GitRepoS\\SparkBoschRepo\\KaggleBosch\\TestData\\labelled_data1.csv",
			sep=";");
    // get first row of 
    val row= data.take(1)
  }

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