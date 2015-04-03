package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Node[K, V, T]
case class SplitNode[K, V, T](val children: Seq[(K, Predicate[V], Node[K, V, T])]) extends Node[K, V, T]
case class LeafNode[K, V, T](
  val index: Int,
  target: T) extends Node[K, V, T]

case class Tree[K, V, T](root: Node[K, V, T]) {
  private def findLeaf(row: Map[K, V], start: Node[K, V, T]): Option[LeafNode[K, V, T]] = {
    start match {
      case leaf: LeafNode[K, V, T] => Some(leaf)
      case SplitNode(children) =>
        children
          .find { case (feature, predicate, _) => predicate(row.get(feature)) }
          .flatMap { case (_, _, child) => findLeaf(row, child) }
    }
  }

  /**
   * Maps the feature keys used to split the `Tree` using `f`.
   */
  def mapKeys[K1](f: K => K1): Tree[K1, V, T] = {
    def recur(node: Node[K, V, T]): Node[K1, V, T] = node match {
      case SplitNode(children) =>
        SplitNode(children.map { case (k, pred, child) =>
          (f(k), pred, recur(child))
        })
      case LeafNode(index, target) => LeafNode(index, target)
    }

    Tree(recur(root))
  }

  /**
   * Maps the [[Predicate]]s in the `Tree` using `f`. Note, this will only
   * produce a valid `Tree` if `f` preserves the ordering (ie if
   * `a.compare(b) == f(a).compare(f(b))`).
   */
  def mapPredicates[V1](f: V => V1)(implicit ord: Ordering[V1]): Tree[K, V1, T] = {
    def mapPred(pred: Predicate[V]): Predicate[V1] = pred match {
      case EqualTo(v) => EqualTo(f(v))
      case LessThan(v) => LessThan(f(v))
      case Not(p) => Not(mapPred(p))
      case AnyOf(ps) => AnyOf(ps.map(mapPred))
    }

    def recur(node: Node[K, V, T]): Node[K, V1, T] = node match {
      case SplitNode(children) =>
        SplitNode(children.map { case (k, pred, child) =>
          (k, mapPred(pred), recur(child))
        })
      case LeafNode(index, target) => LeafNode(index, target)
    }

    Tree(recur(root))
  }

  def leafAt(leafIndex: Int): Option[LeafNode[K, V, T]] = leafAt(leafIndex, root)

  def leafAt(leafIndex: Int, start: Node[K, V, T]): Option[LeafNode[K, V, T]] = {
    start match {
      case leaf: LeafNode[K, V, T] => if (leaf.index == leafIndex) Some(leaf) else None
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
  def prune[P, E](validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): Tree[K, V, T] = {
    Tree(pruneNode(validationData, this.root, voter, error)._2).renumberLeaves
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
  def pruneNode[P, E](validationData: Map[Int, T], start: Node[K, V, T], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T]) = {
    type childSeqType = (K, Predicate[V], Node[K, V, T])
    start match {
      case leaf: LeafNode[K, V, T] => (validationData, leaf) // Bounce at the bottom and start back up the tree.
      case SplitNode(children) => {
        // Call pruneNode on each child, accumulating modified children and additions to the validation data along
        // the way.
        val (newData, newChildren) = children.foldLeft(validationData, Seq[childSeqType]()) {
          case ((vData, childSeq), (k, p, child)) => pruneNode(vData, child, voter, error) match {
            case (v, c) => (v, childSeq :+ (k, p, c))
          }
        }
        // Now that we've taken care of the children, prune the current level.
        val childLeaves = newChildren.collect { case (k, v, s: LeafNode[K, V, T]) => (k, v, s) }
        if (childLeaves.size == newChildren.size) // If all children are leaves, we can potentially prune.
          pruneLevel(SplitNode(newChildren), childLeaves, newData, voter, error)
        else // If any children are SplitNodes, we can't prune.
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
  def pruneLevel[P, E](parent: SplitNode[K, V, T], children: Seq[(K, Predicate[V], LeafNode[K, V, T])], validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit targetMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T]) = {
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
      (validationData + (newIndex -> validationSum), LeafNode(newIndex, targetSum))
    } else {
      (validationData, parent)
    }
  }

  def leafFor(row: Map[K, V]) = findLeaf(row, root)

  def leafIndexFor(row: Map[K, V]) = findLeaf(row, root).map { _.index }

  def targetFor(row: Map[K, V]) = findLeaf(row, root).map { _.target }

  def growByLeafIndex(fn: Int => Seq[(K, Predicate[V], T)]) = {
    var newIndex = -1
    def incrIndex = {
      newIndex += 1
      newIndex
    }

    def growFrom(start: Node[K, V, T]): Node[K, V, T] = {
      start match {
        case LeafNode(index, target) => {
          val newChildren = fn(index)
          if (newChildren.isEmpty)
            LeafNode[K, V, T](incrIndex, target)
          else
            SplitNode[K, V, T](newChildren.map {
              case (feature, predicate, target) =>
                (feature, predicate, LeafNode[K, V, T](incrIndex, target))
            })
        }
        case SplitNode(children) => SplitNode[K, V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, growFrom(child))
        })
      }
    }

    Tree(growFrom(root))
  }

  def updateByLeafIndex(fn: Int => Option[Node[K, V, T]]) = {
    def updateFrom(start: Node[K, V, T]): Node[K, V, T] = {
      start match {
        case LeafNode(index, target) =>
          fn(index).getOrElse(start)
        case SplitNode(children) => SplitNode[K, V, T](children.map {
          case (feature, predicate, child) =>
            (feature, predicate, updateFrom(child))
        })
      }
    }

    Tree(updateFrom(root)).renumberLeaves
  }

  /**
   * Renumber all leaves in the tree.
   *
   * @return A new tree with leaves renumbered.
   */
  def renumberLeaves: Tree[K, V, T] = this.growByLeafIndex { i => Nil }

}

object Tree {
  def empty[K, V, T](t: T): Tree[K, V, T] = Tree(LeafNode[K, V, T](0, t))
  def expand[K, V, T: Monoid](times: Int, leaf: LeafNode[K, V, T], splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T], instances: Iterable[Instance[K, V, T]]): Node[K, V, T] = {
    if (times > 0 && stopper.shouldSplit(leaf.target)) {
      implicit val jdSemigroup = splitter.semigroup

      Semigroup.sumOption(instances.flatMap { instance =>
        instance.features.map { case (f, v) => Map(f -> splitter.create(v, instance.target)) }
      }).flatMap { featureMap =>
        val splits = featureMap.toList.flatMap {
          case (f, s) =>
            splitter.split(leaf.target, s).map { x => f -> evaluator.evaluate(x) }
        }

        val (splitFeature, (split, score)) = splits.maxBy { case (f, (x, s)) => s }
        val edges = split.predicates.toList.map {
          case (pred, _) =>
            val newInstances = instances.filter { inst => pred(inst.features.get(splitFeature)) }
            val target = Monoid.sum(newInstances.map { _.target })
            (pred, target, newInstances)
        }

        if (edges.count { case (pred, target, newInstances) => newInstances.size > 0 } > 1) {
          Some(SplitNode(edges.map {
            case (pred, target, newInstances) =>
              (splitFeature, pred, expand(times - 1, LeafNode[K, V, T](0, target), splitter, evaluator, stopper, newInstances))
          }))
        } else {
          None
        }
      }.getOrElse(leaf)
    } else {
      leaf
    }
  }
}

