package com.stripe.brushfire

import com.twitter.algebird._

trait FrequencyVoter[L] extends Voter[Map[L, Long], Map[L, Double]] {
  def normalize(m: Map[L, Long]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(n, 0L) }
    val total = math.max(nonNeg.values.sum, 1L)
    nonNeg.mapValues { _.toDouble / total }
  }
}

case class SoftVoter[L] extends FrequencyVoter[L] {
  def combine(targets: Iterable[Map[L, Long]]) =
    if (targets.isEmpty)
      Map.empty[L, Double]
    else
      Monoid.sum(targets.map(normalize)).mapValues { _ / targets.size }
}

case class ModeVoter[L] extends FrequencyVoter[L] {
  def mode(m: Map[L, Long]) = m.maxBy { _._2 }._1

  def combine(targets: Iterable[Map[L, Long]]) =
    normalize(Monoid.sum(targets.map { m => Map(mode(m) -> 1L) }))
}

case class ThresholdVoter(threshold: Double, freqVoter: FrequencyVoter[Boolean])
    extends Voter[Map[Boolean, Long], Boolean] {

  def combine(targets: Iterable[Map[Boolean, Long]]) =
    freqVoter.combine(targets).getOrElse(true, 0.0) > threshold
}
