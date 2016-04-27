package com.stripe.brushfire

/** Candidate split for a tree node */
case class Split[V, T](predicate: Predicate[V], leftDistribution: T, rightDistribution: T) {

  def map[U](f: V => U): Split[U, T] =
    Split(predicate.map(f), leftDistribution, rightDistribution)

  /**
   * Given a feature key, create a SplitNode from this Split.
   *
   * Note that the leaves of this node will likely need to be
   * renumbered if this node is put into a larger tree.
   */
  def createSplitNode[K](feature: K): SplitNode[K, V, T, Unit] =
    SplitNode(feature, predicate, LeafNode(0, leftDistribution), LeafNode(1, rightDistribution))
}

object Split {
  /**
   * This is just .map on the Iterable[Split[?, T]] functor
   */
  def wrapSplits[X, T, R](splits: Iterable[Split[X, T]])(fn: X => R): Iterable[Split[R, T]] =
    splits.map(_.map(fn))
}
