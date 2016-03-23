package com.stripe.brushfire
package local

import com.stripe.brushfire._
import com.twitter.algebird._

import AnnotatedTree.AnnotatedTreeTraversal

case class Trainer[K: Ordering, V: Ordering, T: Monoid](
    trainingData: Iterable[Instance[K, V, T]],
    sampler: Sampler[K],
    trees: List[Tree[K, V, T]])(implicit traversal: AnnotatedTreeTraversal[K, V, T, Unit]) {

  private def updateTrees(fn: (Tree[K, V, T], Int, Map[(Int, T, Unit), Iterable[Instance[K, V, T]]]) => Tree[K, V, T]): Trainer[K, V, T] = {
    val newTrees = trees.zipWithIndex.par.map {
      case (tree, index) =>
        val byLeaf =
          trainingData.flatMap { instance =>
            val repeats = sampler.timesInTrainingSet(instance.id, instance.timestamp, index)
            if (repeats > 0) {
              tree.leafFor(instance.features).map { leaf =>
                (1 to repeats).toList.map { i => (instance, leaf) }
              }.getOrElse(Nil)
            } else {
              Nil
            }
          }.groupBy { _._2 }
            .mapValues { _.map { _._1 } }
        fn(tree, index, byLeaf)
    }
    copy(trees = newTrees.toList)
  }

  private def updateLeaves(fn: (Int, (Int, T, Unit), Iterable[Instance[K, V, T]]) => Node[K, V, T, Unit]): Trainer[K, V, T] = {
    updateTrees {
      case (tree, treeIndex, byLeaf) =>
        val newNodes = byLeaf.map {
          case (leaf, instances) =>
            val (index, _, _) = leaf
            index -> fn(treeIndex, leaf, instances)
        }

        tree.updateByLeafIndex(newNodes.lift)
    }
  }

  def updateTargets: Trainer[K, V, T] =
    updateLeaves {
      case (treeIndex, (index, _, annotation), instances) =>
        val target = implicitly[Monoid[T]].sum(instances.map { _.target })
        LeafNode(index, target, annotation)
    }

  def expand(times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]): Trainer[K, V, T] =
    updateLeaves {
      case (treeIndex, (index, target, annotation), instances) =>
        Tree.expand(times, treeIndex, LeafNode[K, V, T, Unit](index, target, annotation), splitter, evaluator, stopper, sampler, instances)
    }

  def validate[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P]): Option[E] = {
    val treeMap = trees.zipWithIndex.map{case (t,i) => i->t}.toMap
    var output: Option[E] = None
    trainingData.foreach{instance =>
      val predictions =
        for (
          (treeIndex, tree) <- treeMap
            if sampler.includeInValidationSet(instance.id, instance.timestamp, treeIndex);
          target <- tree.targetFor(instance.features).toList
        ) yield target

      if(!predictions.isEmpty) {
        val newError = error.create(instance.target, voter.combine(predictions))
        output = output
                    .map{old => error.semigroup.plus(old, newError)}
                    .orElse(Some(newError))
      }
    }

    output
  }
}

object Trainer {
  def apply[K: Ordering, V: Ordering, T: Monoid](trainingData: Iterable[Instance[K, V, T]], sampler: Sampler[K])(implicit traversal: AnnotatedTreeTraversal[K, V, T, Unit]): Trainer[K, V, T] = {
    val empty = 0.until(sampler.numTrees).toList.map { i => Tree.singleton[K, V, T](Monoid.zero) }
    Trainer(trainingData, sampler, empty)
  }
}
