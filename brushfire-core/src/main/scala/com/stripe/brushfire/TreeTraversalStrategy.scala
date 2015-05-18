package com.stripe.brushfire

import scala.annotation.tailrec
import scala.math.Ordering
import scala.util.Random

/**
 * During a tree traversal, searching for a leaf that captures a row, there
 * are times where we have multiple successful candidate paths that can be
 * traversed. There are many ways to choose which path to take, which are
 * packaged up as different `TraversalStrategy`.
 */
trait TraversalStrategy[K, V, T, A] {

  /**
   * Find the [[LeafNode]] that best fits `row` in the tree.  Generally, the
   * path from `tree.root` to the resulting leaf node will be along *only*
   * `true` predicates. However, when multiple predicates are `true` in a
   * [[SplitNode]], the actual choice of which one gets chosen is left to the
   * particular implementation of `TraversalStrategy`.
   *
   * @param tree the decision tree to search in
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leaf node that best matches row
   */
  def find(tree: AnnotatedTree[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] =
    find(tree.root, row)

  /**
   * Find the [[LeafNode]] that best fits `row` and is a descendant of `init`.
   * Generally, the path from `init` to the resulting leaf node will be along
   * *only* `true` predicates. However, when multiple predicates are `true` in
   * a [[SplitNode]], the actual choice of which one gets chosen is left to the
   * particular implementation of `TraversalStrategy`.
   *
   * @param init the initial node to start from
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leaf node that best matches row
   */
  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]]

  /**
   * Walks down the tree, following the true predicates to find the leaf that
   * contains `row`. At each `SplitNode`, this will find all the `true`
   * predicates and call `choose` to resolve these down to at-most 1 node to
   * follow.
   *
   * @param init   the initial node to start the walk at
   * @param row    the "row"/instance we're trying to classify
   * @param choose a function that chooses at-most 1 node from a list of candidates
   * @return the leaf node that best matches row
   */
  protected def findMatching(init: Node[K, V, T, A], row: Map[K, V])(choose: ((Node[K, V, T, A], List[Node[K, V, T, A]])) => Option[Node[K, V, T, A]]): Option[LeafNode[K, V, T, A]] = {
    def recur(node: Node[K, V, T, A]): Option[LeafNode[K, V, T, A]] = {
      node match {
        case leaf @ LeafNode(_, _, _) =>
          Some(leaf)

        case SplitNode(children) =>
          val candidates = children.collect {
            case (key, pred, child) if pred(row.get(key)) => child
          }.toList
          choose(node -> candidates).flatMap(recur)
      }
    }

    recur(init)
  }
}

object TraversalStrategy {

  /** Returns an instance of [[FirstMatch]]. */
  def firstMatch[K, V, T, A]: FirstMatch[K, V, T, A] =
    new FirstMatch[K, V, T, A]

  /** Returns an instance of [[ProbabilisticWeightedMatch]] whose seed is `None`. */
  def probabilisticWeightedMatch[K, V, T, A](implicit toDouble: A => Double): ProbabilisticWeightedMatch[K, V, T, A] =
    new ProbabilisticWeightedMatch[K, V, T, A](None)

  /** Returns an instance of [[MaxWeightedMatch]]. */
  def maxWeightedMatch[K, V, T, A: Ordering]: MaxWeightedMatch[K, V, T, A] =
    new MaxWeightedMatch[K, V, T, A]
}

/**
 * A [[TraversalStrategy]] that will always choose the first matching candidate
 * in a split.
 */
final class FirstMatch[K, V, T, A] extends TraversalStrategy[K, V, T, A] {
  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] =
    findMatching(init, row)(_._2.headOption)
}

/**
 * A [[TraversalStrategy]] that will choose a path probabilistically, weighting
 * the choices by using the annotations as a weight. So, if there are 2
 * candidate paths, `a` and `b`, and `a.annotation` is twice the weight of
 * `b.annotation`, then `a` is twice as likely to be chosen (though `b` may
 * still be chosen!).
 *
 * The actual path chosen depends either on `seed`, if one is provided, or the
 * `row` itself if no `seed` is provided. So, given the same `row`, this should
 * always result in the same leaf being found.
 *
 * @param seed an optional seed for the RNG used in a single traversal
 */
final class ProbabilisticWeightedMatch[K, V, T, A](seed: Option[Long] = None)(implicit toDouble: A => Double) extends TraversalStrategy[K, V, T, A] {
  def withSeed(seed: Long): ProbabilisticWeightedMatch[K, V, T, A] =
    new ProbabilisticWeightedMatch(Some(seed))(toDouble)

  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] = {
    lazy val rng: Random = {
      val random = new Random(seed.getOrElse(row.hashCode.toLong))
      // Hash codes are typically bad random numbers, but Java's Random uses
      // the seed as the first random number so we give spin it first.
      random.nextLong()
      random
    }

    @tailrec
    def loop(children: List[Node[K, V, T, A]], k: Double): Option[Node[K, V, T, A]] =
      children match {
        case child :: rest if k < child.annotation => Some(child)
        case child :: rest => loop(rest, k - child.annotation)
        case Nil => None
      }

    findMatching(init, row) {
      case (_, only :: Nil) => Some(only)
      case (parent, candidates) =>
        loop(candidates, rng.nextDouble * parent.annotation)
    }
  }
}

/**
 * A [[TraversalStrategy]] that will choose the path with the maximum weight
 * amongst the candidates. For instance, if the tree is weighted by the number
 * of instances below each node, then this will always choose the node with the
 * largest number of instances below it.
 */
final class MaxWeightedMatch[K, V, T, A: Ordering] extends TraversalStrategy[K, V, T, A] {
  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] =
    findMatching(init, row) {
      case (_, Nil) => None
      case (_, candidates) => Some(candidates.maxBy(_.annotation))
    }
}
