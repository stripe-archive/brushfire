package com.stripe.brushfire

import com.twitter.algebird._
import com.stripe.bonsai.{ FullBinaryTree, FullBinaryTreeOps }

import java.lang.Math.{abs, max}

sealed abstract class Node[K, V, T, A] {

  def annotation: A

  def renumber(nextId: Int): (Int, Node[K, V, T, A]) =
    this match {
      case SplitNode(k, p, lc0, rc0, a) =>
        val (n1, lc) = lc0.renumber(nextId)
        val (n2, rc) = rc0.renumber(n1)
        (n2, SplitNode(k, p, lc, rc, a))
      case LeafNode(_, target, annotation) =>
        (nextId + 1, LeafNode(nextId, target, annotation))
    }

  def fold[B](f: (Node[K, V, T, A], Node[K, V, T, A], (K, Predicate[V], A)) => B, g: Tuple3[Int, T, A] => B): B =
    this match {
      case SplitNode(k, p, lc, rc, a) => f(lc, rc, (k, p, a))
      case LeafNode(index, target, a) => g((index, target, a))
    }
}

case class SplitNode[K, V, T, A](key: K, predicate: Predicate[V], leftChild: Node[K, V, T, A], rightChild: Node[K, V, T, A], annotation: A) extends Node[K, V, T, A] {
  def evaluate(row: Map[K, V])(implicit ord: Ordering[V]): List[Node[K, V, T, A]] =
    row.get(key) match {
      case Some(v) =>
        (if (predicate(v)) leftChild else rightChild) :: Nil
      case None =>
        leftChild :: rightChild :: Nil
    }

  def splitLabel: (K, Predicate[V], A) = (key, predicate, annotation)
}

object SplitNode {
  def apply[K, V, T, A: Semigroup](k: K, p: Predicate[V], lc: Node[K, V, T, A], rc: Node[K, V, T, A]): SplitNode[K, V, T, A] =
    SplitNode(k, p, lc, rc, Semigroup.plus(lc.annotation, rc.annotation))
}

case class LeafNode[K, V, T, A](index: Int, target: T, annotation: A) extends Node[K, V, T, A] {
  def leafLabel: (Int, T, A) = (index, target, annotation)
}

object LeafNode {
  def apply[K, V, T, A: Monoid](index: Int, target: T): LeafNode[K, V, T, A] =
    LeafNode(index, target, Monoid.zero)
}

case class AnnotatedTree[K, V, T, A](root: Node[K, V, T, A]) {

  import AnnotatedTree.AnnotatedTreeTraversal

  /**
   * Transform the splits of a tree (the predicates and keys of the
   * nodes) while leaving everything else alone.
   */
  private def mapSplits[K0, V0](f: (K, Predicate[V]) => (K0, Predicate[V0])): AnnotatedTree[K0, V0, T, A] = {
    def recur(node: Node[K, V, T, A]): Node[K0, V0, T, A] = {
      node match {
        case SplitNode(k, p, lc, rc, a) =>
          val (key, pred) = f(k, p)
          SplitNode(key, pred, recur(lc), recur(rc), a)
        case LeafNode(index, target, a) =>
          LeafNode(index, target, a)
      }
    }
    AnnotatedTree(recur(root))
  }

  /**
   * Transform the leaves of a tree (the target and annotation) while
   * leaving the structure alone.
   */
  private def mapLeaves[T0, A0: Semigroup](f: (T, A) => (T0, A0)): AnnotatedTree[K, V, T0, A0] = {
    def recur(node: Node[K, V, T, A]): Node[K, V, T0, A0] = node match {
      case SplitNode(k, p, lc, rc, _) =>
        SplitNode(k, p, recur(lc), recur(rc))
      case LeafNode(index, target, a) =>
        val (target1, a1) = f(target, a)
        LeafNode(index, target1, a1)
    }
    AnnotatedTree(recur(root))
  }

  /**
   * Annotate the tree by mapping the leaf target distributions to
   * some annotation for the leaves, then bubbling the annotations up
   * the tree using the `Semigroup` for the annotation type.
   */
  def annotate[A1: Semigroup](f: T => A1): AnnotatedTree[K, V, T, A1] =
    mapLeaves { (t, _) => (t, f(t)) }

  /**
   * Re-annotate the leaves of this tree using `f` to transform the
   * annotations. This will then bubble the annotations up the tree
   * using the `Semigroup` for `A1`. If `f` is a semigroup
   * homomorphism, then this (semantically) just transforms the
   * annotation at each node using `f`.
   */
  def mapAnnotation[A1: Semigroup](f: A => A1): AnnotatedTree[K, V, T, A1] =
    mapLeaves { (t, a) => (t, f(a)) }

  /**
   * Maps the feature keys used to split the `Tree` using `f`.
   */
  def mapKeys[K1](f: K => K1): AnnotatedTree[K1, V, T, A] =
    mapSplits { (k, p) => (f(k), p) }

  /**
   * Maps the [[Predicate]]s in the `Tree` using `f`. Note, this will
   * only produce a valid `Tree` if `f` preserves the ordering (ie if
   * `a.compare(b) == f(a).compare(f(b))`).
   */
  def mapPredicates[V1: Ordering](f: V => V1): AnnotatedTree[K, V1, T, A] =
    mapSplits { (k, p) => (k, p.map(f)) }

  /**
   * Returns the leaf with index `leafIndex` by performing a DFS.
   */
  def leafAt(leafIndex: Int): Option[LeafNode[K, V, T, A]] =
    leafAt(leafIndex, root)

  /**
   * Returns the leaf with index `leafIndex` that is a descendant of
   * `start` by performing a DFS starting with `start`.
   */
  def leafAt(leafIndex: Int, start: Node[K, V, T, A]): Option[LeafNode[K, V, T, A]] =
    start match {
      case SplitNode(k, p, lc, rc, _) =>
        leafAt(leafIndex, lc) match {
          case None => leafAt(leafIndex, rc)
          case some => some
        }
      case leaf @ LeafNode(index, _, _) =>
        if (index == leafIndex) Some(leaf) else None
    }

  /**
   * Prune a tree to minimize validation error.
   *
   * Recursively replaces each split with a leaf when it would have a
   * lower error than the sum of the child leaves errors.
   *
   * @param validationData A Map from leaf index to validation data.
   * @param voter to create predictions from target distributions.
   * @param error to calculate an error statistic given observations (validation) and predictions (training).
   * @return The new, pruned tree.
   */
  def prune[P, E: Ordering](validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit m: Monoid[T], s: Semigroup[A]): AnnotatedTree[K, V, T, A] =
    AnnotatedTree(pruneNode(validationData, root, voter, error)._2)

  /**
   * Prune a tree to minimize validation error, starting from given
   * root node.
   *
   * This method recursively traverses the tree from the root,
   * branching on splits, until it finds leaves, then goes back down
   * the tree combining leaves when such a combination would reduce
   * validation error.
   *
   * @param validationData Map from leaf index to validation data.
   * @param start The root node of the tree.
   * @return A node at the root of the new, pruned tree.
   */
  def pruneNode[P, E: Ordering](validationData: Map[Int, T], start: Node[K, V, T, A], voter: Voter[T, P], error: Error[T, P, E])(implicit m: Monoid[T], aSemigroup: Semigroup[A]): (Map[Int, T], Node[K, V, T, A]) = {
    start match {
      case leaf @ LeafNode(_, _, _) =>
        // Bounce at the bottom and start back up the tree.
        (validationData, leaf)

      case SplitNode(k, p, lc0, rc0, _) =>
        val (vData, lc1) = pruneNode(validationData, lc0, voter, error)
        val (newData, rc1) = pruneNode(vData, rc0, voter, error)

        // If all the children are leaves, we can potentially
        // prune. Otherwise we definitely cannot prune.
        lc1 match {
          case lc2 @ LeafNode(_, _, _) =>
            rc1 match {
              case rc2 @ LeafNode(_, _, _) =>
                pruneLevel(SplitNode(k, p, lc2, rc2), lc2, rc2, newData, voter, error)
              case _ =>
                (newData, SplitNode(k, p, lc1, rc1))
            }
          case _ =>
            (newData, SplitNode(k, p, lc1, rc1))
        }
    }
  }

  /**
   * Test conditions and optionally replace parent with a new leaf
   * that combines children.
   *
   * Also merges validation data for any combined leaves. This relies
   * on a hack that assumes no leaves have negative indices to start
   * out.
   *
   * @param parent
   * @param children
   * @return
   */
  def pruneLevel[P, E](
    parent: SplitNode[K, V, T, A],
    leftChild: LeafNode[K, V, T, A],
    rightChild: LeafNode[K, V, T, A],
    validationData: Map[Int, T],
    voter: Voter[T, P],
    error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T, A]) = {

    def v(leaf: LeafNode[K, V, T, A]): T =
      validationData.getOrElse(leaf.index, targetMonoid.zero)

    def e(leaf: LeafNode[K, V, T, A]): E =
      error.create(v(leaf), voter.combine(Some(leaf.target)))

    val targetSum = targetMonoid.plus(leftChild.target, rightChild.target)
    val validationSum = targetMonoid.plus(v(leftChild), v(rightChild))
    val sumOfErrors = error.semigroup.plus(e(leftChild), e(rightChild))

    // Get training and validation data and validation error for each leaf.
    val targetPrediction = voter.combine(Some(targetSum)) // Generate prediction from combined target.
    val errorOfSums = error.create(validationSum, targetPrediction) // Error of potential combined node.

    // Compare sum of errors and error of sums (and lower us out of the sum of errors Option).
    val doCombine = errorOrdering.gteq(sumOfErrors, errorOfSums)

    if (doCombine) {
      // Create a new leaf from the combination of the children.
      // Find a unique (negative) index for the new leaf:
      val newIndex = -1 * max(abs(leftChild.index), abs(rightChild.index))
      val node = LeafNode[K, V, T, A](newIndex, targetSum, parent.annotation)
      (validationData + (newIndex -> validationSum), node)
    } else {
      (validationData, parent)
    }
  }

  def leafFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: AnnotatedTreeTraversal[K, V, T, A]): Option[(Int, T, A)] =
    traversal.search(this, row, id).headOption

  def leafIndexFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: AnnotatedTreeTraversal[K, V, T, A]): Option[Int] =
    leafFor(row, id).map(_._1)

  def targetFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: AnnotatedTreeTraversal[K, V, T, A], semigroup: Semigroup[T]): Option[T] =
    semigroup.sumOption(leafFor(row, id).map(_._2))

  /**
   * For each leaf, this may convert the leaf to a [[SplitNode]] whose children
   * have the target distribution returned by calling `fn` with the leaf's
   * index. If `fn` returns an empty `Seq`, then the leaf is left as-is.
   */
  def growByLeafIndex(fn: Int => Option[SplitNode[K, V, T, A]])(implicit s: Semigroup[A]): AnnotatedTree[K, V, T, A] = {
    def growFrom(nextIndex: Int, start: Node[K, V, T, A]): (Int, Node[K, V, T, A]) = {
      start match {
        case LeafNode(index, target, annotation) =>
          fn(index) match {
            case None => (nextIndex + 1, LeafNode(nextIndex, target, annotation))
            case Some(split) => split.renumber(nextIndex)
          }
        case SplitNode(k, p, lc0, rc0, _) =>
          val (n1, lc) = growFrom(nextIndex, lc0)
          val (n2, rc) = growFrom(n1, rc0)
          (n2, SplitNode(k, p, lc, rc))
      }
    }

    AnnotatedTree(growFrom(0, root)._2)
  }

  /**
   * For each [[LeafNode]] in this tree, this will replace it with the node
   * returned from `fn` (called with the leaf node's index), if it returns
   * `Some` node. Otherwise, if `fn` returns `None`, then the leaf node is
   * left as-is.
   */
  def updateByLeafIndex(fn: Int => Option[Node[K, V, T, A]])(implicit s: Semigroup[A]): AnnotatedTree[K, V, T, A] = {
    def updateFrom(start: Node[K, V, T, A]): Node[K, V, T, A] = {
      start match {
        case LeafNode(index, _, _) =>
          fn(index).getOrElse(start)
        case SplitNode(k, p, lc0, rc0, _) =>
          SplitNode(k, p, updateFrom(lc0), updateFrom(rc0))
      }
    }

    AnnotatedTree(updateFrom(root)).renumberLeaves
  }

  /**
   * Renumber all leaves in the tree.
   *
   * @return A new tree with leaves renumbered.
   */
  def renumberLeaves: AnnotatedTree[K, V, T, A] =
    AnnotatedTree(root.renumber(0)._2)

  def compress: FullBinaryTree[(K, Predicate[V], A), (Int, T, A)] =
    FullBinaryTree(this)
}

object AnnotatedTree {

  type AnnotatedTreeTraversal[K, V, T, A] = TreeTraversal[AnnotatedTree[K, V, T, A], K, V, T, A]

  implicit def fullBinaryTreeOpsForAnnotatedTree[K, V, T, A]: FullBinaryTreeOps[AnnotatedTree[K, V, T, A], (K, Predicate[V], A), (Int, T, A)] =
    new FullBinaryTreeOpsForAnnotatedTree[K, V, T, A]
}

class FullBinaryTreeOpsForAnnotatedTree[K, V, T, A] extends FullBinaryTreeOps[AnnotatedTree[K, V, T, A], (K, Predicate[V], A), (Int, T, A)] {
  type Node = com.stripe.brushfire.Node[K, V, T, A]

  def root(t: AnnotatedTree[K, V, T, A]): Option[Node] = Some(t.root)

  def foldNode[B](node: Node)(
    f: (Node, Node, (K, Predicate[V], A)) => B,
    g: Tuple3[Int, T, A] => B): B = node.fold(f, g)
}
