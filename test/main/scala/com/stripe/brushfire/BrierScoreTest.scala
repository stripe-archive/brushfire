package com.stripe.brushfire

import org.scalatest._

import com.twitter.algebird._

class BrierScoreTest extends WordSpec with Matchers {
  val EPS = 1e-5
  
  "A BrierScore containing a single example" should {        
    val predicted = Map[(String, Double), Double](("true", 1.0) -> 0.8, ("false", 1.0) -> 0.2)
    val actual = Map[(String, Double), Long](("true", 1.0) -> 1L, ("false", 1.0) -> 0L)
    val counts = Map[(String, Double), Long](("true", 1.0) -> 1L, ("false", 1.0) -> 1L)    
    val totalCount = 1L
    
    val bs = BrierScore[String, Double](predicted, actual, counts, totalCount)
    
    "compute reliability" in {
      bs.reliability shouldEqual 0.08 +- EPS
    }
    
    "compute resolution" in {
      bs.resolution shouldEqual 0.0 +- EPS    
    }
    
    "compute uncertainty" in {
      bs.uncertainty shouldEqual 0.0 +- EPS
    }
    
    "compute the total Brier score" in {
      bs.score shouldEqual 0.08 +- EPS    
    }
  }
}