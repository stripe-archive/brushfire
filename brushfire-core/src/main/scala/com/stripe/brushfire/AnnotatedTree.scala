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
  def evaluate(row: Map[K, V]): List[Node[K, V, T, A]] =
    predicate(row.get(key)) match {
      case Some(true) => leftChild :: Nil
      case Some(false) => rightChild :: Nil
      case None => leftChild :: rightChild :: Nil
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

case class AnnotatedTree[K, V, T, A: Semigroup](root: Node[K, V, T, A]) {

  import AnnotatedTree.AnnotatedTreeTraversal

  /**
   * Transform the splits of a tree (the predicates and keys of the
   * nodes) while leaving everything else alone.
   */
  private def mapSplits[K0, V0](f: (K, Predicate[V]) => (K0, Predicate[V0])): AnnotatedTree[K0, V0, T, A] = {
    def recur(node: Node[K, V, T, A]): Node[K0, V0, T, A] = {
      node match {
        case SplitNode(k, p, lc, rc, _) =>
          val (key, pred) = f(k, p)
          SplitNode(key, pred, recur(lc), recur(rc))
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
  def growByLeafIndex(fn: Int => Option[SplitNode[K, V, T, A]]): AnnotatedTree[K, V, T, A] = {
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
  def updateByLeafIndex(fn: Int => Option[Node[K, V, T, A]]): AnnotatedTree[K, V, T, A] = {
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
