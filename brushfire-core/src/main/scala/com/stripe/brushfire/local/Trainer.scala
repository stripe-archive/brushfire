package com.stripe.brushfire.local

import com.stripe.brushfire._
import com.twitter.algebird._

case class Trainer[K: Ordering, V, T: Monoid](
    trainingData: Iterable[Instance[K, V, T]],
    sampler: Sampler[K],
    trees: List[Tree[K, V, T]]) {

  private def updateTrees(fn: (Tree[K, V, T], Map[LeafNode[K, V, T, Unit], Iterable[Instance[K, V, T]]]) => Tree[K, V, T]) = {
    val newTrees = trees.zipWithIndex.map {
      case (tree, index) =>
        val byLeaf =
          trainingData.flatMap { instance =>
            val repeats = sampler.timesInTrainingSet(instance.id, instance.timestamp, index)
            if (repeats > 0) {
              tree.leafFor(instance.features).map { leaf =>
                1.to(repeats).toList.map { i => (instance, leaf) }
              }.getOrElse(Nil)
            } else {
              Nil
            }
          }.groupBy { _._2 }
            .mapValues { _.map { _._1 } }
        fn(tree, byLeaf)
    }
    copy(trees = newTrees)
  }

  private def updateLeaves(fn: (LeafNode[K, V, T, Unit], Iterable[Instance[K, V, T]]) => Node[K, V, T, Unit]) = {
    updateTrees {
      case (tree, byLeaf) =>
        val newNodes = byLeaf.map {
          case (leaf, instances) =>
            leaf.index -> fn(leaf, instances)
        }

        tree.updateByLeafIndex(newNodes.lift)
    }
  }

  def updateTargets =
    updateLeaves {
      case (leaf, instances) =>
        val target = implicitly[Monoid[T]].sum(instances.map { _.target })
        leaf.copy(target = target)
    }

  def expand(times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]) =
    updateLeaves {
      case (leaf, instances) =>
        Tree.expand(times, leaf, splitter, evaluator, stopper, instances)
    }

  def prune[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P], ord: Ordering[E]) =
    updateTrees {
      case (tree, byLeaf) =>
        val byLeafIndex = byLeaf.map {
          case (l, instances) =>
            l.index -> implicitly[Monoid[T]].sum(instances.map { _.target })
        }
        tree.prune(byLeafIndex, voter, error)
    }

  def validate[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P]): Option[E] = {
    val errors = trainingData.map { instance =>
      val useTrees = trees.zipWithIndex.filter {
        case (tree, i) =>
          sampler.includeInValidationSet(instance.id, instance.timestamp, i)
      }.map { _._1 }
      val prediction = voter.predict(useTrees, instance.features)
      error.create(instance.target, prediction)
    }
    error.semigroup.sumOption(errors)
  }
}

object Trainer {
  def apply[K: Ordering, V, T: Monoid](trainingData: Iterable[Instance[K, V, T]], sampler: Sampler[K]): Trainer[K, V, T] = {
    val empty = 0.until(sampler.numTrees).toList.map { i => Tree.singleton[K, V, T](Monoid.zero) }
    Trainer(trainingData, sampler, empty)
  }
}
