package com.stripe.brushfire

import org.scalatest._

import com.twitter.algebird._

class BrierScoreTest extends WordSpec with Matchers {
  val EPS = 1e-5
    
  "A BrierScore containing a single example" should {
    // A single true example that's predicted to be true with probability 0.8 and false with probability 0.2.
    val predicted = Map[(String, Double), Double](("true", 1.0) -> 0.8, ("false", 1.0) -> 0.2)
    val actual = Map[(String, Double), Long](("true", 1.0) -> 1L, ("false", 1.0) -> 0L)
    val counts = Map[(String, Double), Long](("true", 1.0) -> 1L, ("false", 1.0) -> 1L)    
    val totalCount = 1L
    
    val bs = BrierScore[String, Double](predicted, actual, counts, totalCount)
    
    // reliability = (0.8 - 1.0)^2 + (0.2 - 0.0)^2 = 0.08
    "compute reliability" in {
      bs.reliability shouldEqual 0.08 +- EPS
    }

    // resolution = (1.0 - 1.0)^2 + (0.0 - 0.0)^2 = 0.0
    "compute resolution" in {
      bs.resolution shouldEqual 0.0 +- EPS    
    }
    
    // uncertainty = 1 * (1 - 1) + 0 * (1 - 0) = 0.0
    "compute uncertainty" in {
      bs.uncertainty shouldEqual 0.0 +- EPS
    }
    
    // score = reliability - resolution + uncertainty = 0.08 - 0.0 + 0.0 = 0.08
    "compute the total Brier score" in {
      bs.score shouldEqual 0.08 +- EPS    
    }
  }
  
  "A BrierScore containing two examples" should {
    // One true example that's predicted to be true with probability 0.8 and false with probability 0.2,
    // and one false example that's predicted to be true with probability 0.1 and false with probability 0.9.
    val predicted = Map[(String, Double), Double](("true", 1.0) -> 0.9, ("false", 1.0) -> 1.1)
    val actual = Map[(String, Double), Long](("true", 1.0) -> 1L, ("false", 1.0) -> 1L)
    val counts = Map[(String, Double), Long](("true", 1.0) -> 2L, ("false", 1.0) -> 2L)    
    val totalCount = 2L
    
    val bs = BrierScore[String, Double](predicted, actual, counts, totalCount)
    
    // reliability = 1/2 * [2 * (0.45 - 0.5)^2 + 2 * (0.55 - 0.5)^2] = 0.005
    "compute reliability" in {
      bs.reliability shouldEqual 0.005 +- EPS
    }
    
    // resolution = 1/2 * [2 * (0.5 - 0.5)^2 + 2 * (0.5 - 0.5)^2] = 0.0
    "compute resolution" in {
      bs.resolution shouldEqual 0.0 +- EPS    
    }
    
    // uncertainty = 0.5 * (1 - 0.5) + 0.5 * (1 - 0.5) = 0.5
    "compute uncertainty" in {
      bs.uncertainty shouldEqual 0.5 +- EPS
    }
    
    // score = rel - res + unc = 0.005 - 0.0 + 0.5 = 0.505
    "compute the total Brier score" in {
      bs.score shouldEqual 0.505 +- EPS    
    }
  }
}