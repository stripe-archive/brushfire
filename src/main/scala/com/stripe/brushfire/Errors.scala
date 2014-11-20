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

