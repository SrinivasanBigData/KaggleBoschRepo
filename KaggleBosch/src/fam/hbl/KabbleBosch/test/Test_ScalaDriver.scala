package fam.hbl.KabbleBosch.test

import collection.mutable.Stack  // Here only for example .... To Be Removed
import org.scalatest._

import fam.hbl.Kaggle.Bosch.SparkDriver


class Test_ScalaDriver extends FlatSpec with Matchers {
  
  /** --- Setting the context ------------------------------------
   *  To run the test it is required that there is a spark DataFrame that contains 
   *  some data.  
   */
  
  // create a spark session
  val spark_session= SparkDriver.config_session()
  // some data to create a 
  

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