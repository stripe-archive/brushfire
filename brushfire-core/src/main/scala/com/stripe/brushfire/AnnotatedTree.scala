package com.stripe.brushfire

import scala.annotation.tailrec
import scala.util.Random

import com.twitter.algebird._

sealed trait Node[K, V, T, A] {
  def annotation: A
}

case class SplitNode[K, V, T, A: Semigroup](children: Seq[(K, Predicate[V], Node[K, V, T, A])]) extends Node[K, V, T, A] {
  require(children.nonEmpty)

  lazy val annotation: A =
    Semigroup.sumOption(children.map(_._3.annotation)).get
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
  import AnnotatedTree.defaultTraversalStrategy

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
   * annotation for the leaves, then bubbling the annotaions up the tree using
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
  def mapPredicates[V1](f: V => V1)(implicit ord: Ordering[V1]): AnnotatedTree[K, V1, T, A] = {
    def mapPred(pred: Predicate[V]): Predicate[V1] = pred match {
      case EqualTo(v) => EqualTo(f(v))
      case LessThan(v) => LessThan(f(v))
      case Not(p) => Not(mapPred(p))
      case AnyOf(ps) => AnyOf(ps.map(mapPred))
      case IsPresent(p) => IsPresent(p.map(mapPred))
    }

    mapSplits { (k, p) => (k, mapPred(p)) }
  }

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

  /**
   * Prune a tree to minimize validation error.
   *
   * Recursively replaces each split with a leaf when it would have a lower error than the sum of the child leaves
   * errors.
   *
   * @param validationData A Map from leaf index to validation data.
   * @param voter to create predictions from target distributions.
   * @param error to calculate an error statistic given observations (validation) and predictions (training).
   * @return The new, pruned tree.
   */
  def prune[P, E](validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): AnnotatedTree[K, V, T, A] = {
    AnnotatedTree(pruneNode(validationData, this.root, voter, error)._2).renumberLeaves
  }

  /**
   * Prune a tree to minimize validation error, starting from given root node.
   *
   * This method recursively traverses the tree from the root, branching on splits, until it finds leaves, then goes back
   * down the tree combining leaves when such a combination would reduce validation error.
   *
   * @param validationData Map from leaf index to validation data.
   * @param start The root node of the tree.
   * @return A node at the root of the new, pruned tree.
   */
  def pruneNode[P, E](validationData: Map[Int, T], start: Node[K, V, T, A], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T, A]) = {
    type ChildSeqType = (K, Predicate[V], Node[K, V, T, A])
    start match {
      case leaf @ LeafNode(_, _, _) =>
        // Bounce at the bottom and start back up the tree.
        (validationData, leaf)

      case SplitNode(children) =>
        // Call pruneNode on each child, accumulating modified children and
        // additions to the validation data along the way.
        val (newData, newChildren) =
          children.foldLeft((validationData, Seq[ChildSeqType]())) {
            case ((vData, childSeq), (k, p, child)) =>
              pruneNode(vData, child, voter, error) match {
                case (v, c) => (v, childSeq :+ (k, p, c))
              }
          }
        // Now that we've taken care of the children, prune the current level.
        val childLeaves = newChildren.collect { case (k, v, s @ LeafNode(_, _, _)) => (k, v, s) }

        if (childLeaves.size == newChildren.size) {
          // If all children are leaves, we can potentially prune.
          val parent = SplitNode(newChildren)
          pruneLevel(parent, childLeaves, newData, voter, error)
        } else {
          // If any children are SplitNodes, we can't prune.
          (newData, SplitNode(newChildren))
        }
    }
  }

  /**
   * Test conditions and optionally replace parent with a new leaf that combines children.
   *
   * Also merges validation data for any combined leaves. This relies on a hack that assumes no leaves have negative
   * indices to start out.
   *
   * @param parent
   * @param children
   * @return
   */
  def pruneLevel[P, E](
    parent: SplitNode[K, V, T, A],
    children: Seq[(K, Predicate[V], LeafNode[K, V, T, A])],
    validationData: Map[Int, T],
    voter: Voter[T, P],
    error: Error[T, P, E])(implicit targetMonoid: Monoid[T],
      errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T, A]) = {

    // Get training and validation data and validation error for each leaf.
    val (targets, validations, errors) = children.map {
      case (k, p, leaf) =>
        val trainingTarget = leaf.target
        val validationTarget = validationData.getOrElse(leaf.index, targetMonoid.zero)
        val leafError = error.create(validationTarget, voter.combine(Some(trainingTarget)))
        (trainingTarget, validationTarget, leafError)
    }.unzip3

    val targetSum = targetMonoid.sum(targets) // Combined training targets to create the training data of the potential combined node.
    val targetPrediction = voter.combine(Some(targetSum)) // Generate prediction from combined target.
    val validationSum = targetMonoid.sum(validations) // Combined validation target for combined node.
    val errorOfSums = error.create(validationSum, targetPrediction) // Error of potential combined node.
    val sumOfErrors = error.semigroup.sumOption(errors) // Sum of errors of leaves.
    // Compare sum of errors and error of sums (and lower us out of the sum of errors Option).
    val doCombine = sumOfErrors.exists { sumOE => errorOrdering.gteq(sumOE, errorOfSums) }
    if (doCombine) { // Create a new leaf from the combination of the children.
      // Find a unique (negative) index for the new leaf:
      val newIndex = -1 * children.map { case (k, p, leaf) => Math.abs(leaf.index) }.max
      val node = LeafNode[K, V, T, A](newIndex, targetSum, parent.annotation)
      (validationData + (newIndex -> validationSum), node)
    } else {
      (validationData, parent)
    }
  }

  def leafFor(row: Map[K, V], strategy: TraversalStrategy[K, V, T, A] = defaultTraversalStrategy): Option[LeafNode[K, V, T, A]] =
    strategy.find(root, row)

  def leafIndexFor(row: Map[K, V], strategy: TraversalStrategy[K, V, T, A] = defaultTraversalStrategy): Option[Int] =
    strategy.find(root, row).map { _.index }

  def targetFor(row: Map[K, V], strategy: TraversalStrategy[K, V, T, A] = defaultTraversalStrategy): Option[T] =
    strategy.find(root, row).map { _.target }

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

object AnnotatedTree {
  def defaultTraversalStrategy[K, V, T, A]: TraversalStrategy[K, V, T, A] =
    TraversalStrategy.firstMatch[K, V, T, A]
}
