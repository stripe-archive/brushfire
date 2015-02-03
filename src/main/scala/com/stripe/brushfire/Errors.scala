package com.stripe.brushfire

import com.twitter.algebird._

/**
 * FrequencyError sets up the most common case when dealing
 * with discrete distributions:
 * - normalize each prediction before combining them
 * - normalize to probablities after combining
 * - compute and sum errors separately for each component of the actual distribution
 * - provide a zero for when predictions or actuals are missing
 */
trait FrequencyError[L, E] extends Error[Map[L, Long], E] {

  val semigroup = monoid

  def monoid: Monoid[E]

  def normalizedFrequencies(m: Map[L, Long]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(n, 0L) }
    val total = math.max(nonNeg.values.sum, 1L)
    nonNeg.mapValues { _.toDouble / total }
  }

  def create(actual: Map[L, Long], predicted: Iterable[Map[L, Long]]) = {
    if (predicted.isEmpty)
      monoid.zero
    else {
      val normalized = predicted.map(normalizedFrequencies)
      val probabilities = Monoid.sum(normalized).mapValues { _ / predicted.size }
      monoid.sum(actual.map { case (label, count) => error(label, count, probabilities) })
    }
  }

  def error(label: L, count: Long, probabilities: Map[L, Double]): E
}

case class BrierScoreError[L] extends FrequencyError[L, AveragedValue] {
  lazy val monoid = AveragedValue.group

  def error(label: L, count: Long, probabilities: Map[L, Double]): AveragedValue = {
    val differences = Group.minus(Map(label -> 1.0), probabilities)
    val sumSquareDifferences = differences.values.map { math.pow(_, 2) }.sum
    AveragedValue(count, sumSquareDifferences / math.max(differences.size, 1L))
  }
}

case class BinnedBinaryError
    extends FrequencyError[Boolean, Map[Int, (Long, Long)]] {
  lazy val monoid = implicitly[Monoid[Map[Int, (Long, Long)]]]

  private def percentage(p: Double) = (p * 100).toInt

  def error(label: Boolean, count: Long, probabilities: Map[Boolean, Double]) = {
    val tuple = if (label) (count, 0L) else (0L, count)
    Map(percentage(probabilities.getOrElse(true, 0.0)) -> tuple)
  }
}
