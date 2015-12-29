package com.stripe.brushfire

import com.twitter.algebird._

case class ChiSquaredEvaluator[L, W](implicit weightMonoid: Monoid[W], weightDouble: W => Double)
    extends Evaluator[Map[L, W]] {
  def trainingError(root: Map[L, W], leaves: Iterable[Map[L,W]]) = {
    val rows = leaves.filter { _.nonEmpty }
    if (rows.size > 1) {
      val n = weightMonoid.sum(rows.flatMap { _.values })
      val rowTotals = rows.map { row => weightMonoid.sum(row.values) }.toList
      val columnKeys = rows.flatMap { _.keys }.toList
      val columnTotals = columnKeys.map { column => column -> weightMonoid.sum(rows.flatMap { _.get(column) }) }.toMap
      Some((for {
        column <- columnKeys
        (row, index) <- rows.zipWithIndex
      } yield {
        val observed = row.getOrElse(column, weightMonoid.zero)
        val expected = (columnTotals(column) * rowTotals(index)) / n
        val delta = observed - expected
        (delta * delta) / expected
      }).sum * -1)
    } else
      None
  }
}

case class MinWeightEvaluator[L, W: Monoid](minWeight: W => Boolean, wrapped: Evaluator[Map[L, W]])
    extends Evaluator[Map[L, W]] {
  def trainingError(root: Map[L, W], leaves: Iterable[Map[L,W]]) = {
    if (leaves.forall {freq => minWeight(Monoid.sum(freq.values))})
      wrapped.trainingError(root, leaves)
    else
      None
  }
}

case class ErrorEvaluator[T, P, E](error: Error[T, P, E], voter: Voter[T, P])(fn: E => Double)
    extends Evaluator[T] {
  def trainingError(root: T, leaves: Iterable[T]) = {
    val errors = leaves.map { target => error.create(target, voter.combine(Some(target)))}

    error
      .semigroup
      .sumOption(errors)
      .map{e => fn(e)}
  }
}

case class ExplanationEvaluator[T](wrapped: Evaluator[T])(implicit val group: Group[T], ord: Ordering[T])
  extends Evaluator[T] {

  def trainingError(root: T, leaves: Iterable[T]) = {
    val maxLeaf = leaves.max
    val remainder = group.minus(root, maxLeaf)
    wrapped.trainingError(root, List(maxLeaf, remainder))
  }
}
