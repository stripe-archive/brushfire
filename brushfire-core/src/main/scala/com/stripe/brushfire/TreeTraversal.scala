package com.stripe.brushfire

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.math.Ordering
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.twitter.algebird._
import com.stripe.bonsai._

/**
 * A `TreeTraversal` provides a way to find all of the leaves in a tree that
 * some row can evaluate to. Specifically, there may be cases where multiple
 * predicates in a single split node return `true` for a given row (eg missing
 * features). A tree traversal chooses which paths to go down (which may be all
 * of them) and the order in which they are traversed.
 */
trait TreeTraversal[Tree, K, V, T, A] {

  val treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]

  /**
   * Limit the maximum number of leaves returned from `find` to `n`.
   */
  def limitTo(n: Int): TreeTraversal[Tree, K, V, T, A] =
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
  def search(tree: Tree, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] =
    treeOps.root(tree) match {
      case Some(root) => searchNode(root, row, id)
      case None => Stream.empty
    }

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
  def searchNode(node: treeOps.Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]]
}

/**
 * Simple data type that provides rules to order nodes during
 * traversal.
 *
 * In some cases subtypes of Reorder will also wraps RNG state, for
 * instances that need to randomly select instances. Thus, Reorder is
 * not guaranteed to be referentially-transparent. Fresh instances
 * should be used with each traversal.
 */
trait Reorder[A] {
  def setSeed(seed: Option[String]): Reorder[A]
  def apply[N](n1: N, n2: N, f: N => A): (N, N)
}

object Reorder {

  // Traverse into the left node first.
  def unchanged[A]: Reorder[A] =
    new UnchangedReorder()

  // Traverse into a random node first. Each node has equal
  // probability of being selected.
  def shuffled[A]: Reorder[A] =
    new ShuffledReorder(new Random)

  // Traverse into the node with the higher weight first.
  def weightedDepthFirst[A](implicit ev: Ordering[A]): Reorder[A] =
    new WeightedReorder()

  // Traverse into a random node, but choose the random node based on
  // the ratio between its weight and the total weight of both nodes.
  //
  // If the left node's weight was 10, and the right node's weight was
  // 5, the left node would be picked 2/3 of the time.
  def probabilisticWeightedDepthFirst[A](conversion: A => Double): Reorder[A] =
    new ProbabilisticWeighted(new Random, conversion)

  class UnchangedReorder[A] extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      this
    def apply[N](n1: N, n2: N, f: N => A): (N, N) =
      (n1, n2)
  }

  class ShuffledReorder[A](r: Random) extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      seed match {
        case Some(s) =>
          new ShuffledReorder(new Random(MurmurHash3.stringHash(s)))
        case None =>
          this
      }
    def apply[N](n1: N, n2: N, f: N => A): (N, N) =
      if (r.nextBoolean) (n1, n2) else (n2, n1)
  }

  class WeightedReorder[A](implicit ev: Ordering[A]) extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      this
    def apply[N](n1: N, n2: N, f: N => A): (N, N) =
      if (ev.compare(f(n1), f(n2)) >= 0) (n1, n2) else (n2, n1)
  }

  class ProbabilisticWeighted[A](r: Random, conversion: A => Double) extends Reorder[A] {
    def setSeed(seed: Option[String]): Reorder[A] =
      seed match {
        case Some(s) =>
          new ProbabilisticWeighted(new Random(MurmurHash3.stringHash(s)), conversion)
        case None =>
          this
      }
    def apply[N](n1: N, n2: N, f: N => A): (N, N) = {
      val w1 = conversion(f(n1))
      val w2 = conversion(f(n2))
      val sum = w1 + w2
      if (r.nextDouble * sum < w1) (n1, n2) else (n2, n1)
    }
  }
}

object TreeTraversal {

  def search[Tree, K, V, T, A](tree: Tree, row: Map[K, V], id: Option[String] = None)(implicit ev: TreeTraversal[Tree, K, V, T, A]): Stream[LeafLabel[T, A]] =
    ev.search(tree, row, id)

  /**
   * Performs a depth-first traversal of the tree, returning all matching leaf
   * nodes.
   */
  implicit def depthFirst[Tree, K, V, T, A](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.unchanged)

  /**
   * A depth first search for matching leaves, randomly choosing the order of
   * child candidate nodes to traverse at each step. Since it is depth-first,
   * after a node is chosen to be traversed, all of the matching leafs that
   * descend from that node are traversed before moving onto the node's
   * sibling.
   */
  def randomDepthFirst[Tree, K, V, T, A](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.shuffled)

  /**
   * A depth-first search for matching leaves, where the candidate child nodes
   * for a given parent node are traversed in reverse order of their
   * annotations. This means that if we have multiple valid candidate children,
   * we will traverse the child with the largest annotation first.
   */
  def weightedDepthFirst[Tree, K, V, T, A: Ordering](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.weightedDepthFirst)

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
  def probabilisticWeightedDepthFirst[Tree, K, V, T, A](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]], conversion: A => Double): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.probabilisticWeightedDepthFirst(conversion))
}

case class DepthFirstTreeTraversal[Tree, K, V, T, A](reorder: Reorder[A])(implicit val treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]) extends TreeTraversal[Tree, K, V, T, A] {

  import treeOps.{Node, foldNode}

  def searchNode(start: Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] = {

    // this will be a noop unless we have an id and our reorder
    // instance requires randomness. it ensures that each searchNode
    // call has its own independent RNG (in cases where we care about
    // repeatability, i.e. when `id` is not None).
    val r = reorder.setSeed(id)

    // pull the A value out of a branch or leaf.
    val getAnnotation: Node => A =
      n => foldNode(n)((_, _, bl) => bl._3, ll => ll._3)

    // construct a singleton stream from a leaf
    //val Empty: Stream[LeafLabel[T, A]] = Stream.empty
    val leafF: LeafLabel[T, A] => Stream[LeafLabel[T, A]] =
      _ #:: Stream.empty

    // recurse into branch nodes, going left, right, or both,
    // depending on what our predicate says.
    lazy val branchF: (Node, Node, BranchLabel[K, V, A]) => Stream[LeafLabel[T, A]] =
      { case (lc, rc, (k, p, _)) =>
        p(row.get(k)) match {
          case Some(true) =>
            recurse(lc)
          case Some(false) =>
            recurse(rc)
          case None =>
            val (c1, c2) = r(lc, rc, getAnnotation)
            recurse(c1) #::: recurse(c2)
        }
      }

    // recursively handle each node. the foldNode method decides
    // whether to handle it as a branch or a leaf.
    def recurse(node: Node): Stream[LeafLabel[T, A]] =
      foldNode(node)(branchF, leafF)

    // do it!
    recurse(start)
  }
}

case class LimitedTreeTraversal[Tree, K, V, T, A](traversal: TreeTraversal[Tree, K, V, T, A], limit: Int) extends TreeTraversal[Tree, K, V, T, A] {
  require(limit > 0, "limit must be greater than 0")
  val treeOps: traversal.treeOps.type = traversal.treeOps
  def searchNode(node: treeOps.Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] =
    traversal.searchNode(node, row, id).take(limit)
}
