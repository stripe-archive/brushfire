package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

trait LowPriorityDefaults {
  implicit def dispatchedSplitterWithSparseBoolean[A: Ordering, B, C: Ordering, T](
      implicit ordinal: Splitter[A, T],
      nominal: Splitter[B, T],
      continuous: Splitter[C, T],
      sparse: Splitter[Boolean, T]): Splitter[Dispatched[A, B, C, Boolean], T] =
    DispatchedSplitter(ordinal, nominal, continuous, sparse)
}

trait Defaults extends LowPriorityDefaults {
  implicit def chiSquaredEvaluator[L, W](implicit weightMonoid: Monoid[W], weightDouble: W => Double): Evaluator[Map[L, W]] = ChiSquaredEvaluator[L, W]
  implicit def frequencyStopper[L]: Stopper[Map[L, Long]] = FrequencyStopper(10000, 10)

  implicit def intSplitter[T: Monoid]: Splitter[Int, T] = BinarySplitter[Int, T](LessThan(_))
  implicit def stringSplitter[T: Monoid]: Splitter[String, T] = BinarySplitter[String, T](EqualTo(_))
  implicit def doubleSplitter[T: Monoid]: Splitter[Double, T] = BinnedSplitter(BinarySplitter[Double, T](LessThan(_))) { d => downRez(d, 2, 100) }
  implicit def booleanSplitter[T: Group]: Splitter[Boolean, T] = SparseSplitter[Boolean, T]()

  implicit def dispatchedSplitterWithSpaceSaver[A: Ordering, B, C: Ordering, D, L](
      implicit ordinal: Splitter[A, Map[L, Long]],
      nominal: Splitter[B, Map[L, Long]],
      continuous: Splitter[C, Map[L, Long]]): Splitter[Dispatched[A, B, C, D], Map[L, Long]] =
    DispatchedSplitter(ordinal, nominal, continuous, SpaceSaverSplitter[D, L]())

  implicit def softVoter[L, M: Numeric]: Voter[Map[L, M], Map[L, Double]] = SoftVoter[L, M]()

  def downRez(v: Double, base: Int, precision: Int): Double = {
    if (v == 0.0)
      0.0
    else {
      val sign = math.signum(v)
      val abs = math.abs(v)
      val exponent = math.floor(math.log(abs / precision) / math.log(base))
      val base2Exp = math.pow(base, exponent)
      val mantissa = math.round(abs / base2Exp)
      mantissa * base2Exp * sign
    }
  }
}
