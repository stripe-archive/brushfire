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
   * Recursively replaces each split with a leaf when it would have a lower error than the child leaves.
   *
   * @param validationData Map from leaf index to validation data.
   * @param voter
   * @param error
   * @return The new, pruned tree.
   */
  def prune[P, E](validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit weightMonoid: Monoid[T], errorOrdering: Ordering[E]): Tree[K, V, T] = {
    Tree(pruneNode(validationData, this.root, voter, error)).renumberLeaves
  }

  /**
   * Prune a tree to minimize validation error, starting from given root node.
   *
   * This method recursively traverses the tree from the root, branching on splits, until it finds leaves, then
   * goes back down the tree combining leaves when such a combination would reduce validation error.
   *
   * @param validationData Map from leaf index to validation data.
   * @param start The root node of the tree.
   * @return A node at the root of the new, pruned tree.
   */
  def pruneNode[P, E](validationData: Map[Int, T], start: Node[K, V, T], voter: Voter[T, P], error: Error[T, P, E])(implicit weightMonoid: Monoid[T], errorOrdering: Ordering[E]): Node[K, V, T] = {
    var vData = validationData
    start match {
      case leaf: LeafNode[K, V, T] => leaf // Bounce at the bottom and start back up the tree.
      case SplitNode(children) => {
        val newNode = SplitNode(children.map { case (k, p, n) => (k, p, pruneNode(validationData, n, voter, error)) })
        newNode.children.exists {
          case (k, v, s: SplitNode[K, V, T]) => true // If we have any splits as children, we can't prune.
          case _ => false // If all children are leaves, we can prune.
        } match {
          case true => newNode
          case false => {
            pruneLevel(newNode, newNode.children.asInstanceOf[Seq[(K, Predicate[V], LeafNode[K, V, T])]], vData, voter, error) match {
              case (v, n) => vData = v; n
            }
          }
        }
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
  def pruneLevel[P, E](parent: SplitNode[K, V, T], children: Seq[(K, Predicate[V], LeafNode[K, V, T])], validationData: Map[Int, T], voter: Voter[T, P], error: Error[T, P, E])(implicit weightMonoid: Monoid[T], errorOrdering: Ordering[E]): (Map[Int, T], Node[K, V, T]) = {
    // Get training and validation data and validation error for each leaf.
    val leafData: Seq[(T, T, E)] = children.map {
      case (k, p, leaf) =>
        val validationTarget: T = validationData.getOrElse(leaf.index, weightMonoid.zero)
        val trainingTarget = leaf.target
        val leafError = error.create(validationTarget, voter.combine(Some(trainingTarget)))
        (trainingTarget, validationTarget, leafError)
    }
    val (targets: Seq[T], validations: Seq[T], errors: Seq[E]) = leafData.unzip3
    println()
    println()
    println("targets:", targets)
    println("validations:", validations)
    println("errors:", errors)
    val targetSum: T = weightMonoid.sum(targets) // Combined training targets to create the training data of the potential combined node.
    val targetPrediction: P = voter.combine(Some(targetSum))
    val validationSum: T = weightMonoid.sum(validations) // Combined validation target for combined node.
    println("target sum:", targetSum)
    println("target prediction:", targetPrediction)
    println("validation sum:", validationSum)
    val errorOfSums: E = error.create(validationSum, targetPrediction) // Error of potential combined node.
    val sumOfErrors: Option[E] = error.semigroup.sumOption(errors) // Sum of errors of leaves.
    val doCombine: Boolean = sumOfErrors match {
      case Some(soe) => errorOrdering.gt(soe, errorOfSums)
      case None => false
    }
    println(s"Error of sums = ${errorOfSums} and sum of errors = ${sumOfErrors}. Should we combine? ${doCombine}")
    doCombine match {
      case false => (validationData, parent)
      case true => { // Create a new leaf from the combination of the children.
        println(s"Replacing ${parent}.")
        println(s"Combining ${children}.")
        val newIndex = -1 * Math.abs(children.map { case (k, p, leaf) => leaf.index }.max) // Find a unique (negative) index for the new leaf.
        (validationData + (newIndex -> validationSum), LeafNode(newIndex, weightMonoid.sum(targets)))
      }
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

