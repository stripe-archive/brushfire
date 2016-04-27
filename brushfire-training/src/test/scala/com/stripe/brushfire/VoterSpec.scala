package com.stripe.brushfire

import scala.util.Random

import com.twitter.algebird._

import org.scalacheck.Prop
import org.scalacheck.Prop._
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class VoterSpec extends WordSpec with Matchers with Checkers {
  "soft" should {
    "return empty map on empty iterable" in {
      Voter.soft[String, Int].combine(Nil) shouldBe Map.empty[String, Double]
    }

    "return mean of counts" in {
      val targets = Seq(
        Map(("a", 1), ("b", 2), ("c", 3)),
        Map(("a", 2), ("c", 5)))
      val expected = Map(
        ("a", ((1D / 6D + 2D / 7D) / 2D)),
        ("b", ((2D / 6D) / 2D)),
        ("c", ((3D / 6D + 5D / 7D) / 2D)))
      Voter.soft[String, Int].combine(targets) shouldBe expected
    }
  }

  "mode" should {
    "return empty map on empty iterable" in {
      Voter.mode[String, Int].combine(Nil) shouldBe Map.empty[String, Double]
    }

    "normalize most common labels by target" in {
      val targets = Seq(
        Map("a" -> 1, "b" -> 3, "c" -> 2), // b
        Map("a" -> 2, "c" -> 5), // c
        Map("b" -> 3, "c" -> 4)) // c
      val expected = Map(
        ("b", 1D / 3D),
        ("c", 2D / 3D))
      Voter.mode[String, Int].combine(targets) shouldBe expected
    }
  }

  "mean" should {
    "return 0 for empty iterable" in {
      Voter.mean[Double].combine(Nil) shouldBe 0D
    }

    "return mean of targets" in {
      Voter.mean[Int].combine(Seq(1, 2, 3, 4)) shouldBe (10D / 4D)
      Voter.mean[Double].combine(Seq(1.1, -3, 0.1)) shouldBe (-1.8 / 3D)
    }
  }
}
