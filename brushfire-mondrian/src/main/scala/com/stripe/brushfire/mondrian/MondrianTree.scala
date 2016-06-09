package com.stripe.brushfire.mondrian

import com.twitter.algebird.Semigroup
import scala.util.Random.nextDouble
import scala.collection.mutable.Builder
import scala.math.{ log, max }

/**
 * A Mondrian tree implementation.
 *
 * For details of how the algorithm works, see the paper by
 * Lakshminarayanan et al. [1]
 *
 * [1] http://papers.nips.cc/paper/5234-mondrian-forests-efficient-online-random-forests.pdf
 */
case class MondrianTree[V](root: Option[MondrianTree.Node[V]], λ: Double) {

  import MondrianTree.{ Node, Branch, Leaf, Path }

  /**
   * Return true if the tree is empty.
   */
  def isEmpty: Boolean =
    root.isEmpty

  /**
   * Return true if the tree is non-empty.
   */
  def nonEmpty: Boolean =
    root.nonEmpty

  /**
   * Return the number of the nodes in the tree.
   */
  def size: Int =
    root.map(n => n.fold(_ => 1)(_ + _)).getOrElse(0)

  /**
   * Return the maximum depth of the tree.
   *
   * Since the tree is not balanced, not all paths through the tree
   * will reach this depth.
   */
  def depth: Int =
    root.map(n => n.fold(_ => 1)((x, y) => max(x, y) + 1)).getOrElse(0)

  /**
   * Given the vectors `xss`, compute a histogram of paths.
   */
  def histogram(xss: TraversableOnce[Vector[Double]]): Map[Path[V], Int] =
    xss.foldLeft(Map.empty[Path[V], Int]) { (m, xs) =>
      path(xs).fold(m)(p => m.updated(p, m.getOrElse(p, 0) + 1))
    }

  /**
   * For a single vector `x`, compute its path through the tree.
   *
   * If the tree is empty the result will be `None`. Otherwise, the
   * result will be `Some(path)`.
   */
  def path(x: Vector[Double]): Option[Path[V]] = {
    def descend(node: Node[V], bldr: Builder[Boolean, Vector[Boolean]]): Path[V] =
      node match {
        case leaf @ Leaf(_, _, _, _) =>
          Path(bldr.result, leaf)
        case Branch(i, pt, _, _, _, left, right) =>
          val goRight = x(i) > pt
          descend(if (goRight) right else left, bldr += goRight)
      }
    root.map(node => descend(node, Vector.newBuilder))
  }

  /**
   * Absorb the given vector `xs` into the tree, returning a new tree
   * that has "learned" this point.
   */
  def absorb(xs: Vector[Double], value: V)(implicit s: Semigroup[V]): MondrianTree[V] =
    MondrianTree(Some(root.fold(Node.leaf(λ, xs, value))(n => extendNode(n, xs, value))), λ)

  /**
   * For a given node `t` and vector `x`, do what is necessary to
   * produce a new node that knows about `x`.
   *
   * This method may return a modification of `t`, or it may return a
   * new parent for `t`. This is where the Mondrian tree algorithm is
   * implemented -- the tree grows up from the branches (or leaves),
   * not down from the leaves.
   */
  def extendNode(t: Node[V], x: Vector[Double], value: V)(implicit s: Semigroup[V]): Node[V] = {

    def introduceNewParent(e: Double, timestamp: Double, j: Node[V]): Node[V] = {
      val deltas = Bounds.deltas(j.upperBounds, x, j.lowerBounds)
      val i = Util.weightedSample(deltas)
      val xi = x(i)
      val jubi = j.upperBounds(i)
      val lb = Bounds.min(j.lowerBounds, x)
      val ub = Bounds.max(j.upperBounds, x)
      val leaf = Node.leaf[V](λ, x, value)
      if (xi > jubi) { // cut <= xi
        Branch(i, Util.sample(jubi, xi), timestamp, lb, ub, j, leaf)
      } else { // cut > xi
        Branch(i, Util.sample(xi, j.lowerBounds(i)), timestamp, lb, ub, leaf, j)
      }
    }

    def extendDown(j: Node[V]): Node[V] = {
      val lb = Bounds.min(j.lowerBounds, x)
      val ub = Bounds.max(j.upperBounds, x)
      j match {
        case b @ Branch(i, pt, t, _, _, left, right) =>
          if (x(i) <= pt) {
            b.copy(leftChild = extendBlock(t, left), lowerBounds = lb, upperBounds = ub)
          } else {
            b.copy(rightChild = extendBlock(t, right), lowerBounds = lb, upperBounds = ub)
          }
        case Leaf(t, _, _, value0) =>
          Leaf(t, lb, ub, s.plus(value0, value))
      }
    }

    def extendBlock(parentTimestamp: Double, j: Node[V]): Node[V] = {
      val rate = Bounds.rate(j.upperBounds, x, j.lowerBounds)
      // probability that e > 1/rate is:
      //   exp(-e*rate)
      val e = log(1.0 / nextDouble) / rate
      if (rate > 0.0 && parentTimestamp + e < j.timestamp) {
        introduceNewParent(e, parentTimestamp + e, j)
      } else {
        extendDown(j)
      }
    }

    extendBlock(0.0, t)
  }

  /**
   * Given a function from a leaf's value to a loss, and a way to sum these losses,
   * prune the children of any branch node where the loss of the sum of its children's values
   * is less than the sum of the losses of its children's values.
   * Note: there is originally one E value created per leaf (and these are then recursively summed),
   * which means that E can track the number of leaves involved, if desired, for regularization.
   */
  def pruneBy[E](lossFn: (V,V) => E)(implicit sv: Semigroup[V], se: Semigroup[E], oe: Ordering[E]): MondrianTree[V] = {
    def loop(t: Node[V]): (V, Node[V]) =
      t match {
        case b@Branch(_, _, _, lb, ub, left, right) =>
          val (leftV, newLeft) = loop(left)
          val (rightV, newRight) = loop(right)
          val combinedV = sv.plus(leftV, rightV)
          val unprunedE = se.plus(lossFn(leftV,leftV), lossFn(rightV,rightV))
          val leftPrunedE = lossFn(combinedV, rightV)
          val rightPrunedE = lossFn(combinedV, leftV)
          val minPrunedE = oe.min(leftPrunedE, rightPrunedE)

          if(oe.lt(minPrunedE, unprunedE)) {
           // println((leftV, rightV, leftPrunedE, rightPrunedE, unprunedE))
            if(oe.lt(leftPrunedE, rightPrunedE))
              (rightV, newRight)
            else
              (leftV, newLeft)
          } else {
            val lb = Bounds.min(newLeft.lowerBounds, newRight.lowerBounds)
            val ub = Bounds.max(newLeft.upperBounds, newRight.upperBounds)
            (combinedV, b.copy(leftChild=newLeft, rightChild=newRight))
          }

        case Leaf(_, _, _, v) => (v, t)
      }

    MondrianTree(root.map{r => loop(r)._2}, λ)
  }

  /**
   * Given a vector `x`, find the best prediction available.
   *
   * This prediction will correspond to the mean of the values seen by
   * this leaf.
   */
  def predict(x: Vector[Double]): Option[V] =
    leafFor(x).map(_.value)

  /**
   * Given the vector `x`, find its corresponding leaf and return the
   * center of that leaf's bounding box.
   *
   * The bounding box is an N-dimensional box (a hyper-rectangle), so
   * we find the center by finding the midpoint in each dimension.
   */
  def clusterCenter(x: Vector[Double]): Option[Vector[Double]] =
    leafFor(x).map { case Leaf(_, lb, ub, _) => Bounds.mean(ub, lb) }

  /**
   * Given a vector `x`, find the leaf (if any) that matches.
   *
   * If the tree is empty, this will return `None`. Otherwise it will
   * return `Some(leaf)`
   */
  def leafFor(x: Vector[Double]): Option[Leaf[V]] = {
    def loop(node: Node[V]): Leaf[V] =
      node match {
        case leaf @ Leaf(_, _, _, _) =>
          leaf
        case Branch(i, pt, _, _, _, left, right) =>
          loop(if (x(i) <= pt) left else right)
      }
    root.map(loop)
  }
}

object MondrianTree {

  /**
   * Produce an empty Mondrian tree.
   *
   * Even empty Mondrian tree require a liftime parameter `λ`.
   */
  def empty[V](λ: Double): MondrianTree[V] =
    MondrianTree(None, λ)

  /**
   * Produce a Mondrian tree from a single vector `xs` and a liftime
   * parameter `λ`.
   */
  def apply[V: Semigroup](xs: Vector[Double], value: V, λ: Double): MondrianTree[V] =
    empty[V](λ).absorb(xs, value)

  /**
   * Produce a Mondrian tree from the given vectors `xss` and a
   * liftime parameter `λ`.
   */
  def apply[V: Semigroup](xss: TraversableOnce[Point[V]], λ: Double): MondrianTree[V] =
    xss.foldLeft(empty[V](λ)) { case (t, (xs, v)) => t.absorb(xs, v) }

  /**
   * Produce a Mondrian tree from a single vector `xs` and a liftime
   * parameter `λ`.
   */
  def apply(xs: Vector[Double], λ: Double): MondrianTree[Unit] =
    empty[Unit](λ).absorb(xs, ())

  /**
   * Produce a Mondrian tree from the given vectors `xss` and a
   * liftime parameter `λ`.
   */
  def apply(xss: TraversableOnce[Vector[Double]], λ: Double): MondrianTree[Unit] =
    xss.foldLeft(empty[Unit](λ)) { (t, xs) => t.absorb(xs, ()) }

  /**
   * A node in the mondrian tree.
   *
   * Nodes are either branches or leaves.
   *
   * The timestamp will be in the range [0, λ] (the lambda is the
   * liftime parameter, the limit to how deep the tree can become).
   *
   * The bounds are both inclusive (for a single value the lower and
   * upper bounds will be the same).
   */
  sealed abstract class Node[V] {

    def timestamp: Double
    def lowerBounds: Vector[Double]
    def upperBounds: Vector[Double]

    def fold[A](isLeaf: V => A)(isBranch: (A, A) => A): A =
      this match {
        case Leaf(_, _, _, value) =>
          isLeaf(value)
        case Branch(_, _, _, _, _, left, right) =>
          isBranch(left.fold(isLeaf)(isBranch), right.fold(isLeaf)(isBranch))
      }
  }

  object Node {
    def leaf[V](t: Double, x: Vector[Double], value: V): Node[V] =
      Leaf(t, x, x, value)
  }

  /**
   * Branches are binary splits in the tree.
   *
   * The `splitDim` parameter specifies the index of the feature to
   * split on. It is in the range [0, vector.size).
   *
   * The `splitPoint` parameter specifies the value to split the
   * feature on. If `x <= splitPoint` the left child is chosen,
   * otherwise the right child is chosen.
   */
  case class Branch[V](
    splitDim: Int,
    splitPoint: Double,
    timestamp: Double,
    lowerBounds: Vector[Double],
    upperBounds: Vector[Double],
    leftChild: Node[V],
    rightChild: Node[V]) extends Node[V]

  case class Leaf[V](
    timestamp: Double,
    lowerBounds: Vector[Double],
    upperBounds: Vector[Double],
    value: V) extends Node[V]

  /**
   * Path represents a traversal through the tree to a particular
   * leaf. The decisions represent whether we went left or right,
   * starting from the root.
   */
  case class Path[V](decisions: Vector[Boolean], destination: Leaf[V])
}
