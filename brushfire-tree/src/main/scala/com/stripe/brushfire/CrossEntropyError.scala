package com.stripe.brushfire

import com.twitter.algebird.{AveragedValue, Group, Monoid, Semigroup}

import java.lang.Math.{ log, log1p }

object StableSum {
  def apply(it: Iterator[Double]): Double = {
    var sum = 0.0
    var error = 0.0
    while (it.hasNext) {
      val x = it.next - error
      val next = sum + x
      error = (next - sum) - x
      sum = next
    }
    sum
  }
}

case class CrossEntropyError[L, R](implicit num: Numeric[R]) extends Error[Map[L, R], Map[L, Double], AveragedValue] {

  val semigroup: Semigroup[AveragedValue] = implicitly

  def create(actual: Map[L, R], predicted: Map[L, Double]): AveragedValue = {
    val p = actual.iterator.map { case (k, r) => (k, num.toDouble(r)) }.toMap
    CrossEntropyError.crossEntropyError(p, predicted)
  }
}

object CrossEntropyError {

  def normalize[L, R](p: Map[L, R])(implicit num: Numeric[R]): Map[L, Double] = {
    val total = StableSum(p.values.iterator.map(num.toDouble))
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
    -StableSum(p.iterator.map { case (_, pi) => plogp(pi) })

  // we predicted the distribution was q, but it actually follows distribution p.
  def crossEntropyError[L](p0: Map[L, Double], q0: Map[L, Double]): AveragedValue = {
    val ks = p0.keySet | q0.keySet
    val p = normalize(p0).withDefault(_ => 0.0)
    //val p = p0.withDefault(_ => 0.0)
    val q = clamp(ks, q0)
    //val q = q0.withDefault(_ => Epsilon)
    // val q = ks.iterator.map { k =>
    //   val n = q0.getOrElse(k, 0.0)
    //   if (n < Epsilon) (k, Epsilon) else (k, n)
    // }.toMap
    val score = -StableSum(ks.iterator.map { k => p(k) * log2(q(k)) })
    val count = StableSum(p0.values.iterator).toLong
    AveragedValue(count, score / count)

    // h(p) = - p log p - (1-p) log (1-p) = -(c0/N) log (c0/N) - (c1/N) log (c1/N) =
    //   // log(a/b) = log a - log b
    //   = (-c0 log c0 + c0 log N - c1 log c1 + c1 log N)/N
    // = (-c0 log c0 - c1 log c1)/N + log N
    // g(f(vec(x)))

  }
}
