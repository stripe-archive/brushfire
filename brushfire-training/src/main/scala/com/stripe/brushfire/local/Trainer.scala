package com.stripe.brushfire.local

import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.algebird._

case class Trainer[K: Ordering, V, T: Monoid](
    trainingData: Iterable[Instance[K, V, T]],
    sampler: Sampler[K],
    trees: List[Tree[K, V, T]]) {

  def trainingStep(step: TrainingStep[K,V,T]): Trainer[K,V,T] = {
    val treeMap = trees.zipWithIndex.map{case (t,i) => i->t}.toMap
    var sums1 = Map[(Int,Int,step.K1),step.V1]()

    trainingData.foreach{instance =>
      step.prepare(treeMap, instance).foreach{case (k, v1) =>
        val combined = sums1.get(k) match {
          case Some(old) => step.semigroup1.plus(old, v1)
          case none => v1
        }
        sums1 += k -> combined
      }
    }

    var sums2 = treeMap.mapValues{tree => Map[Int, step.V2]()}
    sums1.foreach{case ((treeIndex, leafIndex, k), v1) =>
      val lifted = step.lift(treeMap(treeIndex), leafIndex, k, v1)

      lifted.foreach{v2 =>
        val combined = sums2(treeIndex).get(leafIndex) match {
          case Some(old) => step.semigroup2.plus(old,v2)
          case none => v2
        }
        sums2 += treeIndex -> (sums2(treeIndex) + (leafIndex -> combined))
      }
    }

    val newTreeMap = sums2.map{case (treeIndex, map) =>
      treeIndex -> step.update(treeMap(treeIndex), sums2(treeIndex))
    }

    val newTrees = 0.until(trees.size).toList.map{i => newTreeMap(i)}

    Trainer(trainingData, sampler, newTrees)
  }

  def outputStep[O](step: OutputStep[K,V,T,O]): Option[O] = {
    val treeMap = trees.zipWithIndex.map{case (t,i) => i->t}.toMap
    var output: Option[O] = None
    trainingData.foreach{instance =>
      step.prepare(treeMap, instance).foreach{o =>
        output = Some(output.map{old => step.semigroup.plus(old, o)}.getOrElse(o))
      }
    }
    output
  }

  def updateTargets =
    trainingStep(UpdateTargets(sampler))

  def expand(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]) =
    trainingStep(Expand(sampler, stopper, splitter, evaluator))

  def validate[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P]): Option[E] =
    outputStep[E](Validate(sampler, error, voter))
}

object Trainer {
  def apply[K: Ordering, V, T: Monoid](trainingData: Iterable[Instance[K, V, T]], sampler: Sampler[K]): Trainer[K, V, T] = {
    val empty = 0.until(sampler.numTrees).toList.map { i => Tree.singleton[K, V, T](Monoid.zero) }
    Trainer(trainingData, sampler, empty)
  }
}
