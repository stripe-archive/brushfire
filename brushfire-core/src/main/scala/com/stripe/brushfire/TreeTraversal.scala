package com.stripe.brushfire

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.math.Ordering
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.twitter.algebird._

/**
 * A `TreeTraversal` provides a way to find all of the leaves in a tree that
 * some row can evaluate to. Specifically, there may be cases where multiple
 * predicates in a single split node return `true` for a given row (eg missing
 * features). A tree traversal chooses which paths to go down (which may be all
 * of them) and the order in which they are traversed.
 */
trait TreeTraversal[K, V, T, A] {

  /**
   * Limit the maximum number of leaves returned from `find` to `n`.
   */
  def limitTo(n: Int): TreeTraversal[K, V, T, A] =
    LimitedTreeTraversal(this, n)

  /**
   * Find the [[LeafNode]]s that best fit `row` in the tree.  Generally, the
   * path from `tree.root` to the resulting leaf node will be along *only*
   * `true` predicates. However, when multiple predicates are `true` in a
   * [[SplitNode]], the actual choice of which ones gets traversed is left to
   * the particular implementation of `TreeTraversal`.
   *
   * @param tree the decision tree to search in
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leaf nodes that best match the row
   */
  def find(tree: AnnotatedTree[K, V, T, A], row: Map[K, V], id: Option[String]): Stream[LeafNode[K, V, T, A]] =
    find(tree.root, row, id)

  /**
   * Find the [[LeafNode]]s that best fit `row` in the tree.  Generally, the
   * path from `node` to the resulting leaf node will be along *only* `true`
   * predicates. However, when multiple predicates are `true` in a
   * [[SplitNode]], the actual choice of which ones gets traversed is left to
   * the particular implementation of `TreeTraversal`.
   *
   * @param node the initial node to start from
   * @param row  the row/instance we're trying to match with a leaf node
   * @return the leaf nodes that match the row
   */
  def find(node: Node[K, V, T, A], row: Map[K, V], id: Option[String]): Stream[LeafNode[K, V, T, A]]
}

object TreeTraversal {

  def find[K, V, T, A](tree: AnnotatedTree[K, V, T, A], row: Map[K, V], id: Option[String] = None)(implicit traversal: TreeTraversal[K, V, T, A]): Stream[LeafNode[K, V, T, A]] =
    traversal.find(tree, row, id)

  /**
   * Performs a depth-first traversal of the tree, returning all matching leaf
   * nodes.
   */
  implicit def depthFirst[K, V, T, A]: TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal((_, xs) => xs)

  /**
   * A depth first search for matching leaves, randomly choosing the order of
   * child candidate nodes to traverse at each step. Since it is depth-first,
   * after a node is chosen to be traversed, all of the matching leafs that
   * descend from that node are traversed before moving onto the node's
   * sibling.
   */
  def randomDepthFirst[K, V, T, A]: TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(_ shuffle _)

  /**
   * A depth-first search for matching leaves, where the candidate child nodes
   * for a given parent node are traversed in reverse order of their
   * annotations. This means that if we have multiple valid candidate children,
   * we will traverse the child with the largest annotation first.
   */
  def weightedDepthFirst[K, V, T, A: Ordering]: TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal((_, xs) => xs.sortBy(_.annotation)(Ordering[A].reverse))

  /**
   * A depth-first search for matching leaves, where the candidate child leaves
   * of a parent node are randomly shuffled, but with nodes with higher weight
   * being given a higher probability of being ordered earlier. This is
   * basically a mix between [[randomDepthFirst]] and [[weightedDepthFirst]].
   *
   * The actual algorithm can best be though of as a random sample from a set
   * of weighted elements without replacement. The weight is directly
   * proportional to its probability of being sampled, relative to all the
   * other elements still in the set.
   */
  def probabilisticWeightedDepthFirst[K, V, T, A](implicit conversion: A => Double): TreeTraversal[K, V, T, A] =
    DepthFirstTreeTraversal(probabilisticShuffle(_, _)(_.annotation))

  // Given a weighted set `xs`, this creates an ordered list of all the elements
  // in `xs` by sampling without replacement from the set, but giving each
  // element a probability of being picked that is equal to its weight / total
  // weight of all elements remaining in the set.
  private[brushfire] def probabilisticShuffle[A](rng: Random, xs: List[A])(getWeight: A => Double): List[A] = {

    @tailrec
    def loop(sum: Double, acc: SortedMap[Double, Int], order: Vector[A], as: List[A]): Vector[A] =
      as match {
        case a :: tail =>
          val sum0 = sum + getWeight(a)
          val newHead =
            if (acc.isEmpty) None
            else acc.from(rng.nextDouble * sum0).headOption
          newHead match {
            case Some((k, i)) =>
              val acc0 = acc + (sum0 -> i) + (k -> order.size)
              loop(sum0, acc0, order.updated(i, a) :+ order(i), tail)
            case None =>
              val acc0 = acc + (sum0 -> order.length)
              loop(sum0, acc0, order :+ a, tail)
          }

        case Nil =>
          order.reverse
      }

    loop(0D, SortedMap.empty, Vector.empty, xs).toList
  }

  def mkRandom(id: String): Random =
    new Random(MurmurHash3.stringHash(id))
}

case class DepthFirstTreeTraversal[K, V, T, A](order: (Random, List[Node[K, V, T, A]]) => List[Node[K, V, T, A]])
    extends TreeTraversal[K, V, T, A] {

  def find(start: Node[K, V, T, A], row: Map[K, V], id: Option[String]): Stream[LeafNode[K, V, T, A]] = {
    // Lazy to avoid creation in the fast case.
    lazy val rng: Random = id.fold[Random](Random)(TreeTraversal.mkRandom)

    // A little indirection makes scalac happy to eliminate some tailcalls in loop.
    def loop0(stack: List[Node[K, V, T, A]]): Stream[LeafNode[K, V, T, A]] =
      loop(stack)

    @tailrec
    def loop(stack: List[Node[K, V, T, A]]): Stream[LeafNode[K, V, T, A]] =
      stack match {
        case Nil =>
          Stream.empty
        case (leaf @ LeafNode(_, _, _)) :: rest =>
          leaf #:: loop0(rest)
        case (split @ SplitNode(_, _, _, _, _)) :: rest =>
          val newStack = split.evaluate(row) match {
            case Nil => rest
            case node :: Nil => node :: rest
            case candidates => order(rng, candidates) ::: rest
          }
          loop(newStack)
      }

    loop0(start :: Nil)
  }
}

case class LimitedTreeTraversal[K, V, T, A](traversal: TreeTraversal[K, V, T, A], limit: Int)
    extends TreeTraversal[K, V, T, A] {
  require(limit > 0, "limit must be greater than 0")

  def find(node: Node[K, V, T, A], row: Map[K, V], id: Option[String]): Stream[LeafNode[K, V, T, A]] =
    traversal.find(node, row, id).take(limit)
}
