package com.stripe.brushfire

import com.twitter.algebird.Semigroup
import org.scalatest.prop.Checkers
import org.scalatest.{ Matchers, WordSpec }

class LazySpec extends WordSpec with Matchers with Checkers {
  "Lazy" should {
    "defer evaluation" in {
      var i = 0
      val l = Lazy { i += 1; i }
      i shouldBe 0
      l.get shouldBe 1
      i shouldBe 1
    }

    "only evaluate once" in {
      var i = 0
      val l = Lazy { i += 1; i }
      i shouldBe 0
      l.get shouldBe 1
      l.get shouldBe 1
      i shouldBe 1
    }

    "defer semigroup plus" in {
      var i = 0
      val l0 = Lazy { i += 1; i }
      val l1 = Lazy { i += 1; i }
      val lp = Semigroup.plus(l0, l1)
      i shouldBe 0
      lp.get shouldBe 3 // 1 + 2
      i shouldBe 2
    }
  }
}
