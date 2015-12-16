package com.stripe.brushfire

import scala.annotation.tailrec
import scala.collection.SortedMap
import scala.math.Ordering
import scala.util.Random
import scala.util.hashing.MurmurHash3

import com.twitter.algebird._
import com.stripe.bonsai._

import Types._

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

trait Reorder[A] {
  def apply[N](r: Random, ns: List[N], f: N => A): List[N]
}

object Reorder {
  def unchanged[A]: Reorder[A] =
    new Reorder[A] {
      def apply[N](r: Random, ns: List[N], f: N => A): List[N] = ns
    }

  def shuffled[A]: Reorder[A] =
    new Reorder[A] {
      def apply[N](r: Random, ns: List[N], f: N => A): List[N] = r.shuffle(ns)
    }

  def weightedDepthFirst[A](implicit ev: Ordering[A]): Reorder[A] =
    new Reorder[A] {
      def apply[N](r: Random, ns: List[N], f: N => A): List[N] = ns.sortBy(f)(ev.reverse)
    }

  def probabilisticWeightedDepthFirst[A](conversion: A => Double): Reorder[A] =
    new Reorder[A] {
      def apply[N](r: Random, ns: List[N], f: N => A): List[N] =
        TreeTraversal.probabilisticShuffle(r, ns)(n => conversion(f(n)))
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

case class DepthFirstTreeTraversal[Tree, K, V, T, A](reorder: Reorder[A])(implicit val treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]) extends TreeTraversal[Tree, K, V, T, A] {

  def searchNode(start: treeOps.Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] = {

    // Lazy to avoid creation in the fast case.
    lazy val rng: Random = id.fold[Random](Random)(TreeTraversal.mkRandom)

    val Empty: Stream[LeafLabel[T, A]] = Stream.empty

    val getAnnotation: treeOps.Node => A =
      n => treeOps.foldNode(n)((_, _, bl) => bl._3, _._3)

    def loop(node: treeOps.Node): Stream[LeafLabel[T, A]] =
      treeOps.foldNode(node)({ case (lc, rc, bl) =>
        val (k, p, a) = bl
        p(row.get(k)) match {
          case Some(true) =>
            loop(lc)
          case Some(false) =>
            loop(rc)
          case None =>
            val cs = reorder(rng, lc :: rc :: Nil, getAnnotation).toStream
            cs.flatMap(loop)
        }
      }, ll => ll #:: Empty)
    loop(start)
  }
}

case class LimitedTreeTraversal[Tree, K, V, T, A](traversal: TreeTraversal[Tree, K, V, T, A], limit: Int) extends TreeTraversal[Tree, K, V, T, A] {
  require(limit > 0, "limit must be greater than 0")
  val treeOps: traversal.treeOps.type = traversal.treeOps
  def searchNode(node: treeOps.Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] =
    traversal.searchNode(node, row, id).take(limit)
}
