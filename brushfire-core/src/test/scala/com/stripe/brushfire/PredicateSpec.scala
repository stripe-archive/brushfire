package com.stripe.brushfire

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class PredicateSpec extends WordSpec with Matchers with Checkers {
  import PredicateGenerators._

  "Predicate" should {
    "!(p(x)) = (!p)(x)" in {
      check { (p: Predicate[Int], x: Int) => !p(x) == (!p).apply(x) }
    }

    "!(!p) = p" in {
      check { (pred: Predicate[Int]) => !(!pred) == pred }
    }

    "can be displayed" in {
      check { (p: Predicate[Int]) => p.display("x") != null }
    }

    "p.map(f)(y) = p(f(y))" in {

    }
  }
}
