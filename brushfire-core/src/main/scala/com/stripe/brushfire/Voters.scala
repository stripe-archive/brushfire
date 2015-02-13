package com.stripe.brushfire

import com.twitter.algebird._

trait FrequencyVoter[L, M] extends Voter[Map[L, M], Map[L, Double]] {
  def normalize[N](m: Map[L, N])(implicit num: Numeric[N]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(num.toDouble(n), 0.0) }
    val total = math.max(nonNeg.values.sum, 1.0)
    nonNeg.mapValues { _ / total }
  }
}

case class SoftVoter[L, M: Numeric]() extends FrequencyVoter[L, M] {
  def combine(targets: Iterable[Map[L, M]]) =
    if (targets.isEmpty)
      Map.empty[L, Double]
    else
      Monoid.sum(targets.map { m => normalize(m) }).mapValues { _ / targets.size }
}

case class ModeVoter[L, M: Ordering]() extends FrequencyVoter[L, M] {
  def mode(m: Map[L, M]): L = m.maxBy { _._2 }._1

  def combine(targets: Iterable[Map[L, M]]) =
    normalize(Monoid.sum(targets.map { m => Map(mode(m) -> 1.0) }))
}

case class ThresholdVoter[M](threshold: Double, freqVoter: FrequencyVoter[Boolean, M])
    extends Voter[Map[Boolean, M], Boolean] {

  def combine(targets: Iterable[Map[Boolean, M]]) =
    freqVoter.combine(targets).getOrElse(true, 0.0) > threshold
}
