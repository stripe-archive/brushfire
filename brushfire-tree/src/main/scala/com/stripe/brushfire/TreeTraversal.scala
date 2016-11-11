package com.stripe.brushfire

import com.stripe.bonsai.FullBinaryTreeOps
import scala.collection.immutable.Stack

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

  def streamNode(node: treeOps.Node): Stream[treeOps.Node]
  def stream(tree: Tree): Stream[treeOps.Node] = treeOps.root(tree) match {
    case Some(root) => streamNode(root)
    case None => Stream.empty
  }
}

object TreeTraversal {

  def search[Tree, K, V, T, A](tree: Tree, row: Map[K, V], id: Option[String] = None)(implicit ev: TreeTraversal[Tree, K, V, T, A]): Stream[LeafLabel[T, A]] =
    ev.search(tree, row, id)

  /**
   * Performs a depth-first traversal of the tree, returning all matching leaf
   * nodes.
   */
  implicit def depthFirst[Tree, K, V: Ordering, T, A](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.unchanged)

  /**
   * A depth-first search for matching leaves, where the candidate child nodes
   * for a given parent node are traversed in reverse order of their
   * annotations. This means that if we have multiple valid candidate children,
   * we will traverse the child with the largest annotation first.
   */
  def weightedDepthFirst[Tree, K, V: Ordering, T, A: Ordering](implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.weightedDepthFirst)

  /**
   * A depth first search for matching leaves, randomly choosing the order of
   * child candidate nodes to traverse at each step. Since it is depth-first,
   * after a node is chosen to be traversed, all of the matching leafs that
   * descend from that node are traversed before moving onto the node's
   * sibling.
   */
  def randomDepthFirst[Tree, K, V: Ordering, T, A](seed: Option[Int] = None)(implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]]): TreeTraversal[Tree, K, V, T, A] =
    DepthFirstTreeTraversal(Reorder.shuffled(seed.getOrElse(System.nanoTime.toInt)))

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
  def probabilisticWeightedDepthFirst[Tree, K, V: Ordering, T, A](seed: Option[Int] = None)(implicit treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]], conversion: A => Double): TreeTraversal[Tree, K, V, T, A] = {
    val n = seed.getOrElse(System.nanoTime.toInt)
    DepthFirstTreeTraversal(Reorder.probabilisticWeightedDepthFirst(n, conversion))
  }
}

case class DepthFirstTreeTraversal[Tree, K, V, T, A](reorder: Reorder[A])(implicit val treeOps: FullBinaryTreeOps[Tree, BranchLabel[K, V, A], LeafLabel[T, A]], ord: Ordering[V]) extends TreeTraversal[Tree, K, V, T, A] {

  import treeOps.{Node, foldNode}

  case class Advancer(stack: List[(Node, Option[Node])]) {
    def hasValue: Boolean = stack.nonEmpty
    def value: Node = stack.head._1
    def advance: Option[Advancer] = stack match {
      case (_, Some(node2)) :: xs => Some(recurse(node2, xs))
      case (_, None) :: Nil => None
      case (_, None) :: xs => Some(Advancer(xs))
      case _ :: Nil => None
      case Nil => None
    }

    val getAnnotation: Node => A =
      node => treeOps.foldNode(node)((_, _, bl) => bl._3, ll => ll._3)

    private def recurse(node: Node, s: List[(Node, Option[Node])]): Advancer = {
      treeOps.foldNode(node)(
        (lc, rc, bl: BranchLabel[K, V, A]) =>
          reorder(lc, rc, getAnnotation, (n1: Node, n2: Node) => recurse(n1, (node, Some(n2)) :: s)),
        ll => Advancer((node, None) :: s)
      )
    }
  }

  object Advancer {
    def fromNode(node: Node): Advancer = Advancer(Nil).recurse(node, Nil)
  }

  def streamNode(node: Node): Stream[Node] = {
    def recurse(advOpt: Option[Advancer]): Stream[Node] = advOpt match {
      case Some(adv) => adv.value #:: recurse(adv.advance)
      case None => Stream.empty
    }
    recurse(Option(Advancer.fromNode(node)))
  }

  def searchNode(start: Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] = {

    // this will be a noop unless we have an id and our reorder
    // instance requires randomness. it ensures that each searchNode
    // call has its own independent RNG (in cases where we care about
    // repeatability, i.e. when `id` is not None).
    val r = reorder.setSeed(id)

    // pull the A value out of a branch or leaf.
    val getAnnotation: Node => A =
      node => foldNode(node)((_, _, bl) => bl._3, ll => ll._3)

    // construct a singleton stream from a leaf
    val leafF: LeafLabel[T, A] => Stream[LeafLabel[T, A]] =
      _ #:: Stream.empty

    // determine the order to traverse into two given nodes. this var
    // is initialized just after 'recurse' -- it is basically a lazy
    // val but with better performance.
    var reorderF: (Node, Node) => Stream[LeafLabel[T, A]] = null

    // recurse into branch nodes, going left, right, or both,
    // depending on what our predicate says. this var is initialized
    // just after 'recurse' -- it is basically a lazy val but with
    // better performance.
    var branchF: (Node, Node, BranchLabel[K, V, A]) => Stream[LeafLabel[T, A]] = null

    // recursively handle each node. the foldNode method decides
    // whether to handle it as a branch or a leaf.
    def recurse(node: Node): Stream[LeafLabel[T, A]] =
      foldNode(node)(branchF, leafF)

    // now that recurse is defined we can initialize this
    reorderF = (n1, n2) => recurse(n1) #::: recurse(n2)

    // now that recurse is defined we can initialize this
    branchF = (lc, rc, t) => t match {
      case (k, p, _) => row.get(k) match {
        case Some(v) => if (p(v)) recurse(lc) else recurse(rc)
        case None => r(lc, rc, getAnnotation, reorderF)
      }
    }

    // ok, now do it!
    //
    // the reason we did all the work above of defining the functions
    // in variables is that this makes our traversal more
    // efficient. otherwise we'd have to generate Function1 instances
    // at each level of each tree.
    recurse(start)
  }
}

case class LimitedTreeTraversal[Tree, K, V, T, A](traversal: TreeTraversal[Tree, K, V, T, A], limit: Int) extends TreeTraversal[Tree, K, V, T, A] {
  require(limit > 0, "limit must be greater than 0")
  val treeOps: traversal.treeOps.type = traversal.treeOps
  def searchNode(node: treeOps.Node, row: Map[K, V], id: Option[String]): Stream[LeafLabel[T, A]] =
    traversal.searchNode(node, row, id).take(limit)
  def streamNode(node: treeOps.Node): Stream[treeOps.Node] = traversal.streamNode(node).take(limit)
}
