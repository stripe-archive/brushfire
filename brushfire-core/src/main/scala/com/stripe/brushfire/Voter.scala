package com.stripe.brushfire

import com.twitter.algebird._

import AnnotatedTree.AnnotatedTreeTraversal

/** Combines multiple targets into a single prediction **/
trait Voter[T, P] { self =>

  /**
   * Combines the targets from multiple trees into a prediction.
   */
  def combine(targets: Iterable[T]): P

  /**
   * Transform the input targets by first applying the function `f`.
   */
  def contramap[S](f: S => T): Voter[S, P] = new Voter[S, P] {
    def combine(targets: Iterable[S]): P =
      self.combine(targets.map(f))
  }

  /**
   * Transform the final predictions of this `Voter` with function `f`.
   */
  def map[Q](f: P => Q): Voter[T, Q] = new Voter[T, Q] {
    def combine(targets: Iterable[T]): Q =
      f(self.combine(targets))
  }

  final def predict[K, V, A](trees: Iterable[AnnotatedTree[K, V, T, A]], row: Map[K, V])(implicit traversal: AnnotatedTreeTraversal[K, V, T, A], semigroup: Semigroup[T]): P =
    combine(trees.flatMap(_.targetFor(row)))
}

object Voter {

  /**
   * Returns a [[Voter]] that uses `f` as the `combine` function.
   */
  def apply[A, B](f: Iterable[A] => B): Voter[A, B] =
    new Voter[A, B] {
      def combine(targets: Iterable[A]): B = f(targets)
    }

  /**
   * returns a [[Voter]] that sums up the targets using the monoid for `T`.
   */
  def fromMonoid[T: Monoid]: Voter[T, T] =
    Voter(Monoid.sum(_))

  /**
   * Returns a [[Voter]] that combines the targets using an aggregator.
   */
  def fromAggregator[A, B, C](aggregate: Aggregator[A, B, C]): Voter[A, C] =
    Voter(aggregate(_))

  type FrequencyVoter[L, M] = Voter[Map[L, M], Map[L, Double]]

  def soft[L, M: Numeric]: FrequencyVoter[L, M] =
    fromMonoid[(Map[L, Double], Int)]
      .contramap[Map[L, M]](normalize(_) -> 1)
      .map { case (m, cnt) => m.mapValues(_ / cnt) }

  def mode[L, M: Ordering]: FrequencyVoter[L, M] =
    fromMonoid[Map[L, Double]]
      .contramap[Map[L, M]](m => Map(mode(m) -> 1.0))
      .map(normalize(_))

  def threshold[M](threshold: Double, voter: FrequencyVoter[Boolean, M]): Voter[Map[Boolean, M], Boolean] =
    voter.map(m => m.getOrElse(true, 0D) > threshold)

  def mean[N: Numeric]: Voter[N, Double] =
    fromAggregator(AveragedValue.numericAggregator[N])

  private def normalize[L, N](m: Map[L, N])(implicit num: Numeric[N]): Map[L, Double] = {
    val nonNeg = m.mapValues { n => math.max(num.toDouble(n), 0.0) }
    val total = math.max(nonNeg.values.sum, 1.0)
    nonNeg.mapValues { _ / total }
  }

  private def mode[L, M: Ordering](m: Map[L, M]): L = m.maxBy { _._2 }._1
}
