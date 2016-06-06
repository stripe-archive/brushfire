package com.stripe.brushfire.mondrian

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
case class MondrianTree(root: Option[MondrianTree.Node], λ: Double) {

  import MondrianTree.{ Node, Branch, Leaf, Path }

  /**
   * Return the number of the nodes in the tree.
   */
  def size: Int =
    root.map(n => n.fold(1)(_ + _)).getOrElse(0)

  /**
   * Return the maximum depth of the tree.
   *
   * Since the tree is not balanced, not all paths through the tree
   * will reach this depth.
   */
  def depth: Int =
    root.map(n => n.fold(1)((x, y) => max(x, y) + 1)).getOrElse(0)

  /**
   * Given the vectors `xss`, compute a histogram of paths.
   */
  def histogram(xss: TraversableOnce[Vector[Double]]): Map[Path, Int] =
    xss.foldLeft(Map.empty[Path, Int]) { (m, xs) =>
      path(xs).fold(m)(p => m.updated(p, m.getOrElse(p, 0) + 1))
    }

  /**
   * For a single vector `x`, compute its path through the tree.
   *
   * If the tree is empty the result will be `None`. Otherwise, the
   * result will be `Some(path)`.
   */
  def path(x: Vector[Double]): Option[Path] = {
    def descend(node: Node, bldr: Builder[Boolean, Vector[Boolean]]): Path =
      node match {
        case leaf @ Leaf(_, _, _) =>
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
  def absorb(xs: Vector[Double]): MondrianTree =
    MondrianTree(Some(root.fold(Node.leaf(λ, xs))(n => extendNode(n, xs))), λ)

  /**
   * For a given node `t` and vector `x`, do what is necessary to
   * produce a new node that knows about `x`.
   *
   * This method may return a modification of `t`, or it may return a
   * new parent for `t`. This is where the Mondrian tree algorithm is
   * implemented -- the tree grows up from the branches (or leaves),
   * not down from the leaves.
   */
  def extendNode(t: Node, x: Vector[Double]): Node = {

    def introduceNewParent(e: Double, timestamp: Double, j: Node): Node = {
      val deltas = Bounds.deltas(j.upperBounds, x, j.lowerBounds)
      val i = Util.weightedSample(deltas)
      val xi = x(i)
      val jubi = j.upperBounds(i)
      val lb = Bounds.min(j.lowerBounds, x)
      val ub = Bounds.max(j.upperBounds, x)
      val leaf = Node.leaf(λ, x)
      if (xi > jubi) { // cut <= xi
        Branch(i, Util.sample(jubi, xi), timestamp, lb, ub, j, leaf)
      } else { // cut > xi
        Branch(i, Util.sample(xi, j.lowerBounds(i)), timestamp, lb, ub, leaf, j)
      }
    }

    def extendDown(j: Node): Node = {
      val lb = Bounds.min(j.lowerBounds, x)
      val ub = Bounds.max(j.upperBounds, x)
      j match {
        case b @ Branch(i, pt, t, _, _, left, right) =>
          if (x(i) <= pt) {
            b.copy(leftChild = extendBlock(t, left), lowerBounds = lb, upperBounds = ub)
          } else {
            b.copy(rightChild = extendBlock(t, right), lowerBounds = lb, upperBounds = ub)
          }
        case leaf @ Leaf(_, _, _) =>
          leaf.copy(lowerBounds = lb, upperBounds = ub)
      }
    }

    def extendBlock(parentTimestamp: Double, j: Node): Node = {
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
}

object MondrianTree {

  /**
   * Produce an empty Mondrian tree.
   *
   * Even empty Mondrian tree require a liftime parameter `λ`.
   */
  def empty(λ: Double): MondrianTree =
    MondrianTree(None, λ)

  /**
   * Produce a Mondrian tree from a single vector `xs` and a liftime
   * parameter `λ`.
   */
  def apply(xs: Vector[Double], λ: Double): MondrianTree =
    empty(λ).absorb(xs)

  /**
   * Produce a Mondrian tree from the given vectors `xss` and a
   * liftime parameter `λ`.
   */
  def apply(xss: TraversableOnce[Vector[Double]], λ: Double): MondrianTree =
    xss.foldLeft(MondrianTree.empty(λ))((t, xs) => t.absorb(xs))

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
  sealed abstract class Node {

    def timestamp: Double
    def lowerBounds: Vector[Double]
    def upperBounds: Vector[Double]

    def fold[A](isLeaf: A)(isBranch: (A, A) => A): A =
      this match {
        case Leaf(_, _, _) =>
          isLeaf
        case Branch(_, _, _, _, _, left, right) =>
          isBranch(left.fold(isLeaf)(isBranch), right.fold(isLeaf)(isBranch))
      }
  }

  object Node {
    def leaf(t: Double, x: Vector[Double]): Node = Leaf(t, x, x)
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
  case class Branch(
    splitDim: Int,
    splitPoint: Double,
    timestamp: Double,
    lowerBounds: Vector[Double],
    upperBounds: Vector[Double],
    leftChild: Node,
    rightChild: Node) extends Node

  case class Leaf(
    timestamp: Double,
    lowerBounds: Vector[Double],
    upperBounds: Vector[Double]) extends Node

  /**
   * Path represents a traversal through the tree to a particular
   * leaf. The decisions represent whether we went left or right,
   * starting from the root.
   */
  case class Path(decisions: Vector[Boolean], destination: Leaf)
}
