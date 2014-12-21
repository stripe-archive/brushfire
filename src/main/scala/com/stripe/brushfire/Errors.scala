package com.stripe.brushfire

import com.twitter.algebird._


 case class BinnedError[B, T: Monoid](binner: Iterable[T] => B) extends Error[T, Map[B, T]] {
  val semigroup = implicitly[Semigroup[Map[B, T]]]
  val ordering = Ordering.by{err: Map[B, T] => 1} //TODO: this is awful
  def create(actual: T, predicted: Iterable[T]) = Map(binner(predicted) -> actual)
 }

object Errors {
  def averageProbability(predicted: Iterable[Map[Boolean, Long]]): Double = {
    if (predicted.size == 0)
      0.0
    else {
      val scores = predicted.map { m =>
        val trues = math.max(m.getOrElse(true, 0L).toDouble, 0.0)
        val falses = math.max(m.getOrElse(false, 0L).toDouble, 0.0)
        if (trues == 0.0)
          0.0
        else
          trues / (falses + trues)
      }
      scores.sum / scores.size
    }
  }

  def averagePercentage(predicted: Iterable[Map[Boolean, Long]]): Double =
    Math.floor(averageProbability(predicted) * 100) / 100.0
}

case class BrierScoreError[L] extends Error[Map[L, Long], AveragedValue] {
  val semigroup = AveragedValue.group
  val ordering = Ordering.by { a: AveragedValue => a.value }

  def normalizedFrequencies(m: Map[L, Long]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(n, 0L) }
    val total = math.max(nonNeg.values.sum, 1L)
    nonNeg.mapValues { _.toDouble / total }
  }

  def create(actual: Map[L, Long], predicted: Iterable[Map[L, Long]]): AveragedValue = {
    predicted match {
      case Nil => AveragedValue(0L, 0.0)
      case _ =>
        val probs = predicted.map(normalizedFrequencies)
        val averagedScores = Monoid.sum(probs).mapValues { _ / predicted.size }
        val count = actual.values.sum
        val allLabels = averagedScores.keys.toSet ++ actual.keys.toSet
        Monoid.sum(allLabels.flatMap { label =>
          val matches = actual.getOrElse(label, 0L)
          val prob = averagedScores.getOrElse(label, 0.0)
          List(
            AveragedValue(matches, math.pow(1.0 - prob, 2)),
            AveragedValue(count - matches, math.pow(prob, 2)))
        })
    }
  }
}
