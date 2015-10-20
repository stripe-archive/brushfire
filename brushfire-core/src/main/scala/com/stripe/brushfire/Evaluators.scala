package com.stripe.brushfire

import com.twitter.algebird._

case class ChiSquaredEvaluator[V, L, W, A](implicit weightMonoid: Monoid[W], weightDouble: W => Double)
    extends Evaluator[V, Map[L, W], A] {
  def evaluate(split: Split[V, Map[L, W], A]) = {
    val rows = split.predicates.map { _._2 }.filter { _.nonEmpty }
    if (rows.size > 1) {
      val n = weightMonoid.sum(rows.flatMap { _.values })
      val rowTotals = rows.map { row => weightMonoid.sum(row.values) }.toList
      val columnKeys = rows.flatMap { _.keys }.toList
      val columnTotals = columnKeys.map { column => column -> weightMonoid.sum(rows.flatMap { _.get(column) }) }.toMap
      val testStatistic = (for {
        column <- columnKeys
        (row, index) <- rows.zipWithIndex
      } yield {
        val observed = row.getOrElse(column, weightMonoid.zero)
        val expected = (columnTotals(column) * rowTotals(index)) / n
        val delta = observed - expected
        (delta * delta) / expected
      }).sum
      (split, testStatistic)
    } else
      (EmptySplit[V, Map[L, W], A](), Double.NegativeInfinity)
  }
}

case class MinWeightEvaluator[V, L, W: Monoid, A](minWeight: W => Boolean, wrapped: Evaluator[V, Map[L, W], A])
    extends Evaluator[V, Map[L, W], A] {
  def evaluate(split: Split[V, Map[L, W], A]) = {
    val (baseSplit, baseScore) = wrapped.evaluate(split)
    if (baseSplit.predicates.forall {
      case (pred, freq, annotation) =>
        val totalWeight = Monoid.sum(freq.values)
        minWeight(totalWeight)
    })
      (baseSplit, baseScore)
    else
      (EmptySplit[V, Map[L, W], A](), Double.NegativeInfinity)
  }
}

case class EmptySplit[V, P, A]() extends Split[V, P, A] {
  val predicates = Nil
}

case class ErrorEvaluator[V, T, P, E, A](error: Error[T, P, E], voter: Voter[T, P])(fn: E => Double)
    extends Evaluator[V, T, A] {
  def evaluate(split: Split[V, T, A]) = {
    val totalErrorOption =
      error.semigroup.sumOption(
        split
          .predicates
          .map { case (_, target, _) => error.create(target, voter.combine(Some(target))) })

    totalErrorOption match {
      case Some(totalError) => (split, -fn(totalError))
      case None => (EmptySplit[V, T, A](), Double.NegativeInfinity)
    }
  }
}
