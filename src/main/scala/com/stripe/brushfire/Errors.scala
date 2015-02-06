package com.stripe.brushfire

import com.twitter.algebird._

/**
 * FrequencyError sets up the most common case when dealing
 * with discrete distributions:
 * - compute and sum errors separately for each component of the actual distribution
 * - provide a zero for when predictions or actuals are missing
 */
trait FrequencyError[L, M, E] extends Error[Map[L, M], Map[L, Double], E] {

  val semigroup = monoid

  def monoid: Monoid[E]

  def create(actual: Map[L, M], predicted: Map[L, Double]) = {
    if (predicted.isEmpty)
      monoid.zero
    else {
      monoid.sum(actual.map { case (label, count) => error(label, count, predicted) })
    }
  }

  def error(label: L, count: M, probabilities: Map[L, Double]): E
}

case class BrierScoreError[L, M](implicit num: Numeric[M])
    extends FrequencyError[L, M, AveragedValue] {
  lazy val monoid = AveragedValue.group

  def error(label: L, count: M, probabilities: Map[L, Double]): AveragedValue = {
    val differences = Group.minus(Map(label -> 1.0), probabilities)
    val sumSquareDifferences = differences.values.map { math.pow(_, 2) }.sum
    AveragedValue(num.toLong(count), sumSquareDifferences / math.max(differences.size, 1L))
  }
}

case class BinnedBinaryError[M: Monoid]
    extends FrequencyError[Boolean, M, Map[Int, (M, M)]] {
  lazy val monoid = implicitly[Monoid[Map[Int, (M, M)]]]

  private def percentage(p: Double) = (p * 100).toInt

  def error(label: Boolean, count: M, probabilities: Map[Boolean, Double]) = {
    val tuple = if (label) (count, Monoid.zero[M]) else (Monoid.zero[M], count)
    Map(percentage(probabilities.getOrElse(true, 0.0)) -> tuple)
  }
}
