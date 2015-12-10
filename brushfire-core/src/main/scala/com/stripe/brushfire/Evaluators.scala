package com.stripe.brushfire

import com.twitter.algebird._

case class ChiSquaredEvaluator[V, L, W](implicit weightMonoid: Monoid[W], weightDouble: W => Double)
    extends Evaluator[V, Map[L, W]] {
  def evaluate(split: Split[V, Map[L, W]]): Option[(Split[V, Map[L, W]], Double)] = {
    val rows = split.distributions.filter { _.nonEmpty }
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
      Some((split, testStatistic))
    } else {
      None
    }
  }
}

case class MinWeightEvaluator[V, L, W: Monoid](minWeight: W => Boolean, wrapped: Evaluator[V, Map[L, W]])
    extends Evaluator[V, Map[L, W]] {

  private[this] def test(dist: Map[L, W]): Boolean = minWeight(Monoid.sum(dist.values))

  def evaluate(split: Split[V, Map[L, W]]): Option[(Split[V, Map[L, W]], Double)] =
    wrapped.evaluate(split).filter { case (baseSplit, _) =>
      test(baseSplit.leftDistribution) && test(baseSplit.rightDistribution)
    }
}

case class ErrorEvaluator[V, T, P, E](error: Error[T, P, E], voter: Voter[T, P])(fn: E => Double) extends Evaluator[V, T] {
  def evaluate(split: Split[V, T]): Option[(Split[V, T], Double)] = {
    def e(t: T): E = error.create(t, voter.combine(Some(t)))
    val e0 = e(split.leftDistribution)
    val e1 = e(split.rightDistribution)
    Some((split, -fn(error.semigroup.plus(e0, e1))))
  }
}
