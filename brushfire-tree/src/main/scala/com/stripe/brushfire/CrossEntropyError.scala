package com.stripe.brushfire

import com.twitter.algebird.{AveragedValue, Group, Monoid, Semigroup}

import java.lang.Math.{ log, log1p }

case class CrossEntropyError[L, R](implicit num: Numeric[R]) extends Error[Map[L, R], Map[L, Double], (Double, Long)] {

  val semigroup: Semigroup[(Double, Long)] = implicitly[Semigroup[(Double, Long)]]

  def create(actual: Map[L, R], predicted: Map[L, Double]): (Double, Long) = {
    val p = actual.iterator.map { case (k, r) => (k, num.toDouble(r)) }.toMap
    CrossEntropyError.crossEntropyError(p, predicted)
  }
}

object CrossEntropyError {

  def normalize[L, R](p: Map[L, R])(implicit num: Numeric[R]): Map[L, Double] = {
    val total = p.iterator.map { case (_, r) => num.toDouble(r) }.sum
    p.iterator.map { case (k, r) => (k, num.toDouble(r) / total) }.toMap
  }

  val Epsilon: Double = 1e-6

  def clamp[L](ks: Set[L], p: Map[L, Double]): Map[L, Double] =
    normalize(ks.iterator.map { k =>
      val n = p.getOrElse(k, 0.0)
      if (n < Epsilon) (k, Epsilon) else (k, n)
    }.toMap)

  val OneOverLogTwo: Double = 1.0 / log(2.0)

  def log2(n: Double): Double =
    if (n > 0.5) log1p(n - 1.0) * OneOverLogTwo
    else log(n) * OneOverLogTwo

  def plogp(n: Double): Double =
    if (n <= 0.0) 0.0
    else if (n >= 1.0) 0.0
    else n * log2(n)

  def entropy[L](p: Map[L, Double]): Double =
    -p.iterator.map { case (_, pi) => plogp(pi) }.sum

  // we assumed the distribution was q, but it actually follows distribution p.
  def crossEntropyError[L](p0: Map[L, Double], q0: Map[L, Double]): (Double, Long) = {
    val ks = p0.keySet | q0.keySet
    val p = normalize(p0).withDefault(_ => 0.0)
    val q = clamp(ks, q0)
    (-ks.iterator.map { k => p(k) * log2(q(k)) }.sum, 1L)
  }

}
