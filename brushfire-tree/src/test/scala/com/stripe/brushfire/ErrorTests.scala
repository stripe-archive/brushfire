package com.stripe.brushfire

import com.twitter.algebird.{ AveragedValue, Semigroup }

import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks

import Arbitrary.arbitrary
import Ordering.Implicits._

// TODO: it's not clear these laws are either necessary or sufficient
//       to express what we actually need an Error type to do. we'll
//       hang onto them now until we can find something better.

abstract class ErrorTests[L: Arbitrary, T: Arbitrary: Semigroup, E: Ordering]
    extends PropSpec with Matchers with PropertyChecks {

  type Err <: Error[T, Map[L, Double], E]
  val err: Err
  import err.{ create, semigroup }

  // implements scalatest comparisons, returns unit if things are ok
  def basicallyEq(x: Double, y: Double, eps: Double): Unit = {
    import java.lang.Double.MIN_NORMAL
    val xa = x.abs
    val ya = y.abs
    val d = (xa - ya).abs
    if (x != y) {
      val div = if (x == 0 || y == 0 || d < MIN_NORMAL) MIN_NORMAL else (xa + ya)
      (d / div) should be < eps
    } else ()
  }

  // this is used to see if things are "close enough"
  // it's fairly hand-wavey but better than nothing
  def nearEq(e1: E, e2: E): Unit

  property("create(x) + create(y) = create(x + y)") {
    forAll { (a1: T, a2: T, d: Distribution[L]) =>
      val p = d.toMap
      val lhs = semigroup.plus(create(a1, p), create(a2, p))
      val rhs = create(Semigroup.plus(a1 ,a2), p)
      nearEq(lhs, rhs)
    }
  }

  property("create(x) = create(x + x)") {
    forAll { (a: T, d: Distribution[L]) =>
      val p = d.toMap
      val lhs = create(a, p)
      val rhs = create(Semigroup.plus(a, a), p)
      nearEq(lhs, rhs)
    }
  }

  property("create(x) <= create(x + y) <= create(y) when create(x) <= create(y)") {
    forAll { (a1: T, a2: T, d: Distribution[L]) =>
      val p = d.toMap
      val e1 = create(a1, p)
      val e2 = create(a2, p)
      val (x, y) = if (e1 <= e2) (e1, e2) else (e2, e1)
      val xy = semigroup.plus(x, y)
      x should be <= xy
      xy should be <= y
    }
  }
}

object Arbitraries {

  implicit val orderingAveragedValue: Ordering[AveragedValue] =
    Ordering.by(_.value)
}

import Arbitraries._

// L = Boolean
// M = Count
class BrierScoreErrorTest extends ErrorTests[Boolean, Map[Boolean, Count], AveragedValue] {

  type Err = BrierScoreError[Boolean, Count]
  val err = BrierScoreError[Boolean, Count]

  def nearEq(e1: AveragedValue, e2: AveragedValue): Unit =
    basicallyEq(e1.value, e2.value, 1e-10)
}

// it seems like cross-entropy doesn't satisfy these laws
//
// class CrossEntropyErrorTest extends ErrorTests[Boolean, Map[Boolean, Count], AveragedValue] {
//
//   type Err = CrossEntropyError[Boolean, Count]
//   val err = CrossEntropyError[Boolean, Count]
//
//   def nearEq(e1: AveragedValue, e2: AveragedValue): Unit =
//     basicallyEq(e1.value, e2.value, 1e-3)
// }

class Distribution[L](items0: Map[L, Double]) {
  val toMap: Map[L, Double] = {
    val nonNeg = items0.mapValues(_.abs)
    val total = nonNeg.values.sum
    nonNeg.map { case (k, v) => (k, v / total) }
  }
}

object Distribution {

  implicit def arbitraryDistribution[L: Arbitrary]: Arbitrary[Distribution[L]] =
    Arbitrary(arbitrary[Map[L, Double]]
      .filter(_.nonEmpty)
      .map(new Distribution(_)))
}

case class Count(toLong: Long) {
  require(toLong > 0L)
  def +(that: Count): Count = Count(this.toLong + that.toLong)
}

object Count {

  // count must be positive
  implicit val arbitraryCount: Arbitrary[Count] =
    Arbitrary(arbitrary[Int].map(n => Count((n & 0x7fffffffL) + 1L)))

  // this is a pretty fake numeric, fwiw
  implicit val numericCount: Numeric[Count] =
    new Numeric[Count] {
      def fromInt(n: Int): Count = Count(n)
      def negate(x: Count): Count = sys.error("!!")
      def plus(x: Count, y: Count): Count = x + y
      def minus(x: Count, y: Count): Count = Count(x.toLong - y.toLong)
      def times(x: Count, y: Count): Count = Count(x.toLong * y.toLong)
      def toInt(x: Count): Int = x.toLong.toInt
      def toLong(x: Count): Long = x.toLong
      def toFloat(x: Count): Float = x.toLong.toFloat
      def toDouble(x: Count): Double = x.toLong.toDouble
      def compare(x: Count, y: Count): Int =
        x.toLong compare y.toLong
    }

  implicit val semigroupCount: Semigroup[Count] =
    new Semigroup[Count] {
      def plus(x: Count, y: Count): Count = x + y
    }
}
