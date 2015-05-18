package com.stripe.brushfire

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

class PredicateSpec extends WordSpec with Matchers with Checkers {
  import PredicateGenerators._

  "Predicate" should {
    "allow missing values in all but IsPresent" in {
      check { (pred: Predicate[Int]) =>
        pred match {
          case IsPresent(_) => pred(None) == false
          case _ => pred(None) == true
        }
      }
    }

    "Not negates the predicate" in {
      check { (pred: Predicate[Int], value: Int) =>
        !pred(Some(value)) == Not(pred)(Some(value))
      }
    }

    "can be displayed" in {
      check { (pred: Predicate[Int]) =>
        // We really just don't want it to blow up.
        Predicate.display(pred) != null
      }
    }
  }
}
