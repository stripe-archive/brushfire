package com.stripe.brushfire

import scala.annotation.tailrec
import scala.math.Ordering
import scala.util.Random

import com.twitter.algebird._

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

        case split @ SplitNode(_) =>
          choose(node -> findCandidates(split, row)).flatMap(recur)
      }
    }

    recur(init)
  }

  /**
   * Returns all the child nodes of `node` whose predicates are `true` for the
   * given `row` (the candidates).
   *
   * @param node the split node to find the candidate children of
   * @param row  the row being evaluated
   * @return list of candidate child nodes of `node`
   */
  protected def findCandidates(node: SplitNode[K, V, T, A], row: Map[K, V]): List[Node[K, V, T, A]] =
    node.children.collect {
      case (key, pred, child) if pred(row.get(key)) => child
    }.toList
}

object TraversalStrategy {

  /** Returns an instance of [[FirstMatch]]. */
  def firstMatch[K, V, T, A]: FirstMatch[K, V, T, A] =
    new FirstMatch[K, V, T, A]

  /** Returns an instance of [[ProbabilisticWeightedMatch]] whose seed is `None`. */
  def probabilisticWeightedMatch[K, V, T, A](seed: Long)(implicit toDouble: A => Double): ProbabilisticWeightedMatch[K, V, T, A] =
    new ProbabilisticWeightedMatch[K, V, T, A](seed)

  /** Returns an instance of [[MaxWeightedMatch]]. */
  def maxWeightedMatch[K, V, T, A: Ordering]: MaxWeightedMatch[K, V, T, A] =
    new MaxWeightedMatch[K, V, T, A]

  /**
   * Returns an [[TraversalStrategy]] that always chooses the candidate path
   * that results in the largest *target* (type `T`). This may have to traverse
   * each candidate path fully in order to find the resulting target value.
   *
   * Optionally, it may be possible to short-circuit some traversals if the
   * node annotation's contain information about the maximum target in any
   * descendant leaf nodes. This is specified by providing a `getMaxTarget`
   * function that returns a value rather than the default of `None`.
   *
   * @param getMaxTarget function that returns the maximum target in a subtree
   */
  def maxTargetMatch[K, V, T: Ordering, A](getMaxTarget: A => Option[Max[T]] = (_: A) => None): MaxTargetMatch[K, V, T, A] =
    new MaxTargetMatch[K, V, T, A]({ a =>
      getMaxTarget(a) match {
        case Some(max) => Some(max.get)
        case None => None
      }
    })

  /**
   * Returns an [[TraversalStrategy]] that always chooses the candidate path
   * that results in the smallest *target* (type `T`). This may have to traverse
   * each candidate path fully in order to find the resulting target value.
   *
   * Optionally, it may be possible to short-circuit some traversals if the
   * node annotation's contain information about the minimum target in any
   * descendant leaf nodes. This is specified by providing a `getMinTarget`
   * function that returns a value rather than the default of `None`.
   *
   * @param getMinTarget function that returns the minimum target in a subtree
   */
  def minTargetMatch[K, V, T: Ordering, A](getMinTarget: A => Option[Min[T]] = (_: A) => None): MaxTargetMatch[K, V, T, A] =
    new MaxTargetMatch[K, V, T, A]({ a =>
      getMinTarget(a) match {
        case Some(min) => Some(min.get)
        case None => None
      }
    })(Ordering[T].reverse)
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
 * A [[TraversalStrategy]] that will choose the path with the smallest target
 * value. This will will always traverse all candidate paths completely, so it
 * should be used with caution, as it could impact performance.
 */
final class MaxTargetMatch[K, V, T, A](getMaxTarget: A => Option[T])(implicit ord: Ordering[T]) extends TraversalStrategy[K, V, T, A] {

  /**
   * Let path finding short circuit if we can get the maximum target of a given
   * node in constant time (ie from the annotaion). This requires that
   * `maxTarget` returns the maximum target of all descendant leaf nodes.
   */
  def withShortCircuit(maxTarget: A => Option[T]): MaxTargetMatch[K, V, T, A] =
    new MaxTargetMatch(maxTarget)

  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] = {
    def recur(nodes: List[Node[K, V, T, A]], max: Option[LeafNode[K, V, T, A]]): Option[LeafNode[K, V, T, A]] =
      nodes match {
        case Nil =>
          max
        case LeafNode(_, target, _) :: rest if max.isDefined && ord.lt(target, max.get.target) =>
          recur(rest, max)
        case (leaf @ LeafNode(_, _, _)) :: rest =>
          recur(rest, Some(leaf))
        case (node @ SplitNode(_)) :: rest =>
          val newMax = getMaxTarget(node.annotation) match {
            case Some(maxInPath) if max.isDefined && ord.lt(maxInPath, max.get.target) =>
              // We couldn't possibly do better than max, so skip this node.
              max
            case _ =>
              // Tackle the nodes with the highest potential targets first and
              // hope this heuristic let's us avoid future traversals.
              val childCandidates = findCandidates(node, row)
                .sortBy(c => getMaxTarget(c.annotation))
                .reverse
              maxLeaf(max, recur(childCandidates, max))
          }
          recur(rest, newMax)
      }

    recur(init :: Nil, None)
  }

  private def maxLeaf(a: Option[LeafNode[K, V, T, A]], b: Option[LeafNode[K, V, T, A]]): Option[LeafNode[K, V, T, A]] =
    (a, b) match {
      case (Some(a0), Some(b0)) if ord.gteq(a0.target, b0.target) => a
      case (_, None) => a
      case _ => b
    }
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
final class ProbabilisticWeightedMatch[K, V, T, A](seed: Long)(implicit toDouble: A => Double) extends TraversalStrategy[K, V, T, A] {
  def find(init: Node[K, V, T, A], row: Map[K, V]): Option[LeafNode[K, V, T, A]] = {
    lazy val rng: Random = {
      val random = new Random(seed)
      // Java's Random uses the seed as the first random number, so we give it
      // spin to get something useful.
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
