package com.stripe.brushfire.mondrian

import com.twitter.algebird.{Semigroup, Monoid}
import scala.util.Random.nextDouble
import scala.collection.mutable.Builder
import scala.math.{ log, max }

/**
 * A Mondrian forest representation.
 *
 * The forest is represented as a list of trees. It supports basically
 * the same operations that trees do, aggregating the individual
 * trees' results into a single value to return.
 */
case class MondrianForest[V](trees: List[MondrianTree[V]]) {

  /**
   * Absorb a particular vector and value into each of the trees in
   * this forest.
   */
  def absorb(xs: Vector[Double], value: V)(implicit m: Monoid[V]): MondrianForest[V] =
    MondrianForest(trees.map(t => t.absorb(xs, value)))

  /**
   * Return a predictive for the given vector `xs` by combining the
   * predictions of all the subtress of this forest.
   *
   * Monoid[V] is used to combine the values.
   */
  def predict(xs: Vector[Double])(implicit m: Monoid[V]): V =
    m.sum(trees.iterator.map(_.predict(xs)).flatten)

  /**
   * Find the mean of the cluster centers of `xs` in this forest's
   * subtrees.
   *
   * The mean is calculated from the sum of the non-empty trees'
   * centers, divided by the number of non-empty trees.
   */
  def clusterCenter(xs: Vector[Double]): Option[Vector[Double]] = {
    val centers = trees.iterator.map(_.clusterCenter(xs)).flatten.toList
    if (centers.isEmpty) None else {
      val totals = centers.reduceLeft(Bounds.add)
      Some(totals.map(_ / centers.size))
    }
  }

  def pruneBy[E](lossFn: (V,V) => E)(implicit sv: Semigroup[V], se: Semigroup[E], oe: Ordering[E]): MondrianForest[V] =
    MondrianForest(trees.map(_.pruneBy(lossFn)))
}

object MondrianForest {

  def empty[V](trees: Int, λ: Double): MondrianForest[V] =
    MondrianForest(List.fill(trees)(MondrianTree.empty(λ)))

  def apply[V: Monoid](trees: Int, xss: TraversableOnce[Point[V]], λ: Double): MondrianForest[V] = {
    val e = MondrianForest.empty[V](trees, λ)
    xss.foldLeft(e) { case (t, (xs, v)) => t.absorb(xs, v) }
  }
}
