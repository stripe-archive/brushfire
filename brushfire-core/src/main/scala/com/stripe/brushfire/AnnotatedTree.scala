package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Node[K, V, T, A] {
  def annotation: A
}

case class SplitNode[K, V, T, A: Semigroup](children: Seq[(K, Predicate[V], Node[K, V, T, A])]) extends Node[K, V, T, A] {
  require(children.nonEmpty)

  lazy val annotation: A =
    Semigroup.sumOption(children.map(_._3.annotation)).get

  def findChildren(row: Map[K, V]): List[Node[K, V, T, A]] =
    children.collect { case (k, p, n) if p(row.get(k)) => n }(collection.breakOut)
}

case class LeafNode[K, V, T, A](
  index: Int,
  target: T,
  annotation: A) extends Node[K, V, T, A]

object LeafNode {
  def apply[K, V, T, A: Monoid](index: Int, target: T): LeafNode[K, V, T, A] =
    LeafNode(index, target, Monoid.zero)
}

case class AnnotatedTree[K, V, T, A: Semigroup](root: Node[K, V, T, A]) {
  private def mapSplits[K0, V0](f: (K, Predicate[V]) => (K0, Predicate[V0])): AnnotatedTree[K0, V0, T, A] = {
    def recur(node: Node[K, V, T, A]): Node[K0, V0, T, A] = node match {
      case SplitNode(children) =>
        SplitNode(children.map {
          case (key, pred, child) =>
            val (key0, pred0) = f(key, pred)
            (key0, pred0, recur(child))
        })

      case LeafNode(index, target, annotation) =>
        LeafNode(index, target, annotation)
    }

    AnnotatedTree(recur(root))
  }

  private def mapLeaves[T0, A0: Semigroup](f: (T, A) => (T0, A0)): AnnotatedTree[K, V, T0, A0] = {
    def recur(node: Node[K, V, T, A]): Node[K, V, T0, A0] = node match {
      case SplitNode(children) =>
        SplitNode(children.map {
          case (key, pred, child) =>
            (key, pred, recur(child))
        })

      case LeafNode(index, target, annotation) =>
        val (target1, annotation1) = f(target, annotation)
        LeafNode(index, target1, annotation1)
    }

    AnnotatedTree(recur(root))
  }

  /**
   * Annotate the tree by mapping the leaf target distributions to some
   * annotation for the leaves, then bubbling the annotations up the tree using
   * the `Semigroup` for the annotation type.
   */
  def annotate[A1: Semigroup](f: T => A1): AnnotatedTree[K, V, T, A1] =
    mapLeaves { (t, _) => (t, f(t)) }

  /**
   * Re-annotate the leaves of this tree using `f` to transform the
   * annotations. This will then bubble the annotations up the tree using the
   * `Semigroup` for `A1`. If `f` is a semigroup homomorphism, then this
   * (semantically) just transforms the annotation at each node using `f`.
   */
  def mapAnnotation[A1: Semigroup](f: A => A1): AnnotatedTree[K, V, T, A1] =
    mapLeaves { (t, a) => (t, f(a)) }

  /**
   * Maps the feature keys used to split the `Tree` using `f`.
   */
  def mapKeys[K1](f: K => K1): AnnotatedTree[K1, V, T, A] =
    mapSplits { (k, p) => (f(k), p) }

  /**
   * Maps the [[Predicate]]s in the `Tree` using `f`. Note, this will only
   * produce a valid `Tree` if `f` preserves the ordering (ie if
   * `a.compare(b) == f(a).compare(f(b))`).
   */
  def mapPredicates[V1: Ordering](f: V => V1): AnnotatedTree[K, V1, T, A] =
    mapSplits { (k, p) => (k, p.map(f)) }

  /**
   * Returns the leaf with index `leafIndex` by performing a DFS.
   */
  def leafAt(leafIndex: Int): Option[LeafNode[K, V, T, A]] = leafAt(leafIndex, root)

  /**
   * Returns the leaf with index `leafIndex` that is a descendant of `start` by
   * performing a DFS starting with `start`.
   */
  def leafAt(leafIndex: Int, start: Node[K, V, T, A]): Option[LeafNode[K, V, T, A]] = {
    start match {
      case leaf @ LeafNode(_, _, _) =>
        if (leaf.index == leafIndex) Some(leaf) else None
      case SplitNode(children) =>
        children
          .flatMap { case (_, _, child) => leafAt(leafIndex, child) }
          .headOption
    }
  }

  def leafFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: TreeTraversal[K, V, T, A]): Option[LeafNode[K, V, T, A]] =
    traversal.find(root, row, id).headOption

  def leafIndexFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: TreeTraversal[K, V, T, A]): Option[Int] =
    leafFor(row, id).map(_.index)

  def targetFor(row: Map[K, V], id: Option[String] = None)(implicit traversal: TreeTraversal[K, V, T, A], semigroup: Semigroup[T]): Option[T] =
    semigroup.sumOption(traversal.find(root, row, id).map(_.target))

  /**
   * For each leaf, this may convert the leaf to a [[SplitNode]] whose children
   * have the target distribution returned by calling `fn` with the leaf's
   * index. If `fn` returns an empty `Seq`, then the leaf is left as-is.
   */
  def growByLeafIndex(fn: Int => Seq[(K, Predicate[V], T, A)]): AnnotatedTree[K, V, T, A] = {
    var newIndex = -1
    def incrIndex(): Int = {
      newIndex += 1
      newIndex
    }

    def growFrom(start: Node[K, V, T, A]): Node[K, V, T, A] = {
      start match {
        case LeafNode(index, target, annotation) =>
          val newChildren = fn(index)
          if (newChildren.isEmpty)
            LeafNode[K, V, T, A](incrIndex(), target, annotation)
          else
            SplitNode[K, V, T, A](newChildren.map {
              case (feature, predicate, target, childAnnotation) =>
                val child = LeafNode[K, V, T, A](incrIndex(), target, childAnnotation)
                (feature, predicate, child)
            })

        case SplitNode(children) =>
          SplitNode[K, V, T, A](children.map {
            case (feature, predicate, child) =>
              (feature, predicate, growFrom(child))
          })
      }
    }

    AnnotatedTree(growFrom(root))
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
        case SplitNode(children) =>
          SplitNode[K, V, T, A](children.map {
            case (feature, predicate, child) =>
              (feature, predicate, updateFrom(child))
          })
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
    this.growByLeafIndex { i => Nil }
}
