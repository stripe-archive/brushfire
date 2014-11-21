package com.stripe.brushfire

import com.twitter.algebird._

case class BinnedError[B, T: Monoid](binner: Iterable[T] => B) extends Error[T, Map[B, T]] {
  val semigroup = implicitly[Semigroup[Map[B, T]]]

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

case class BrierScoreError[A] extends Error[Map[A, Long], AveragedValue] {
  val semigroup = AveragedValue.group

  def normalizedFrequencies(m: Map[A, Long]): Map[A, Double] = {
    // Make this at least one to handle empty map case
    val total = math.max(m.values.sum, 1L)
    m.mapValues(_.toDouble / total)
  }

  def create(actual: Map[A, Long], predicted: Iterable[Map[A, Long]]): AveragedValue = {
    // actual is expected to be a Map[class -> indicator]
    // where indicator is {0,1} for membership of this instance to the class
    predicted match {
      case Nil => AveragedValue(0L, 0.0)
      case _ =>
        val probs = predicted.map(normalizedFrequencies)
        val averagedScores = Monoid.sum(probs).mapValues(_ / predicted.size)
        val differences: Map[A, Double] = new MapGroup[A, Double].minus(actual.mapValues(_.toDouble), averagedScores)
        val sumSquareDifferences = differences.values.map { math.pow(_, 2) }.sum
        AveragedValue(1L, sumSquareDifferences / math.max(differences.size, 1L))
    }
  }
}
