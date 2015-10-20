package com.stripe.brushfire.local

import com.stripe.brushfire._
import com.twitter.algebird._

case class Trainer[M, K: Ordering, V, T: Monoid, A: Semigroup](
    trainingData: Iterable[Instance[M, Map[K, V], T]],
    sampler: Sampler[M, K],
    trees: List[Tree[K, V, T, A]]) {

  private def updateTrees(fn: (Tree[K, V, T, A], Map[LeafNode[K, V, T, A], Iterable[Instance[M, Map[K, V], T]]]) => Tree[K, V, T, A]): Trainer[M, K, V, T, A] = {
    val newTrees = trees.zipWithIndex.par.map {
      case (tree, index) =>
        val byLeaf =
          trainingData.flatMap { instance =>
            val repeats = sampler.timesInTrainingSet(instance.metadata, index)
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
    copy(trees = newTrees.toList)
  }

  private def updateLeaves(fn: (LeafNode[K, V, T, A], Iterable[Instance[M, Map[K, V], T]]) => Node[K, V, T, A]): Trainer[M, K, V, T, A] = {
    updateTrees {
      case (tree, byLeaf) =>
        val newNodes = byLeaf.map {
          case (leaf, instances) =>
            leaf.index -> fn(leaf, instances)
        }

        tree.updateByLeafIndex(newNodes.lift)
    }
  }

  def updateTargets: Trainer[M, K, V, T, A] =
    updateLeaves {
      case (leaf, instances) =>
        val target = implicitly[Monoid[T]].sum(instances.map { _.target })
        leaf.copy(target = target)
    }

  def expand(times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T, A], stopper: Stopper[T], annotator: Annotator[M, A]): Trainer[M, K, V, T, A] =
    updateLeaves {
      case (leaf, instances) =>
        Tree.expand(times, leaf, splitter, evaluator, stopper, annotator, instances)
    }

  def prune[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P], ord: Ordering[E]): Trainer[M, K, V, T, A] =
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
          sampler.includeInValidationSet(instance.metadata, i)
      }.map { _._1 }
      val prediction = voter.predict(useTrees, instance.features)
      error.create(instance.target, prediction)
    }
    error.semigroup.sumOption(errors)
  }
}

object Trainer {
  def apply[M, K: Ordering, V, T: Monoid](
      trainingData: Iterable[Instance[M, Map[K, V], T]],
      sampler: Sampler[M, K]): Trainer[M, K, V, T, Unit] = {
    val empty = 0.until(sampler.numTrees).toList.map { i => Tree.singleton[K, V, T, Unit](Monoid.zero[T], ()) }
    Trainer(trainingData, sampler, empty)
  }
}
