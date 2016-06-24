package com.stripe.brushfire
package local

import com.stripe.brushfire._
import com.twitter.algebird._

import AnnotatedTree.AnnotatedTreeTraversal


//map with a reservoir of up to `capacity` randomly chosen keys
case class SampledMap[A,B](capacity: Int) {
  var mapValues = Map[A,B]()
  var randValues = Map[A,Double]()
  var threshold = 0.0
  val rand = new util.Random

  private def randValue(key: A): Double = {
    randValues.get(key) match {
      case Some(r) => r
      case None => {
        val r = rand.nextDouble
        randValues += key->r

        if(randValues.size <= capacity && r >= threshold)
          threshold = r
        else if(randValues.size > capacity && r < threshold) {
          println("evicting")
          val bottomK = randValues.toList.sortBy{_._2}.take(capacity)
          val keep = bottomK.map{_._1}.toSet
          threshold = bottomK.last._2
          mapValues = mapValues.filterKeys(keep)
        }

        r
      }
    }
  }

  def containsKey(key: A): Boolean = randValue(key) <= threshold
  def update(key: A, value: B) {
    if(containsKey(key))
      mapValues += key -> value
  }

  def get(key: A): Option[B] = mapValues.get(key)
}

case class Trainer[K: Ordering, V: Ordering, T: Monoid](
    trainingData: Iterable[Instance[K, V, T]],
    sampler: Sampler[K],
    trees: List[Tree[K, V, T]])(implicit traversal: AnnotatedTreeTraversal[K, V, T, Unit]) {

  val treeMap = trees.zipWithIndex.map{case (t,i) => i->t}.toMap

  def expand(maxLeavesPerTree: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]): Trainer[K, V, T] = {
    val allStats = treeMap.map{case (treeIndex, tree) =>
      treeIndex -> SampledMap[Int,Map[K,splitter.S]](maxLeavesPerTree)
    }

    trainingData.foreach{instance =>
      lazy val features = instance.features.mapValues { value => splitter.create(value, instance.target) }

      for (
        (treeIndex, tree) <- treeMap.toList;
        treeStats <- allStats.get(treeIndex).toList;
        i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
        (leafIndex, target, annotation) <- tree.leafFor(instance.features).toList
          if stopper.shouldSplit(target) && treeStats.containsKey(leafIndex);
        (feature, stats) <- features
          if sampler.includeFeature(feature, treeIndex, leafIndex)
      ) {
        var leafStats = treeStats.get(leafIndex).getOrElse(Map[K,splitter.S]())
        val combined = leafStats.get(feature) match {
          case Some(old) => splitter.semigroup.plus(old, stats)
          case None => stats
        }
        leafStats += feature -> combined
        treeStats.update(leafIndex, leafStats)
      }
    }

    val newTreeMap = allStats.map{case (treeIndex, treeStats) =>
      val tree = treeMap(treeIndex)
      treeIndex -> tree.growByLeafIndex{leafIndex =>
        val candidates = for(
          leafStats <- treeStats.get(leafIndex).toList;
          parent <- tree.leafAt(leafIndex).toList;
          (feature, stats) <- leafStats.toList;
          split <- splitter.split(parent.target, stats);
          (newSplit, score) <- evaluator.evaluate(split).toList
        ) yield (newSplit.createSplitNode(feature), score)

        if(candidates.isEmpty)
          None
        else
          Some(candidates.maxBy{_._2}._1)
      }
    }

    val newTrees = 0.until(trees.size).toList.map{i => newTreeMap(i)}
    Trainer(trainingData, sampler, newTrees)
  }

  def updateTargets: Trainer[K, V, T] = {
    var targets = treeMap.mapValues{tree => Map[Int, T]()}
    trainingData.foreach{instance =>
      for (
        (treeIndex, tree) <- treeMap.toList;
        i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
        leafIndex <- tree.leafIndexFor(instance.features).toList
      ) {
        val treeTargets = targets(treeIndex)
        val old = treeTargets.getOrElse(leafIndex, Monoid.zero[T])
        val combined = Monoid.plus(instance.target, old)
        targets += treeIndex -> (treeTargets + (leafIndex -> combined))
      }
    }

    val newTrees = trees.zipWithIndex.map{case (tree, index) =>
      tree.updateByLeafIndex{leafIndex =>
        val target = targets(index).getOrElse(leafIndex, Monoid.zero[T])
        Some(LeafNode(leafIndex, target, ()))
      }
    }

    copy(trees = newTrees)
  }

  def validate[P, E](error: Error[T, P, E])(implicit voter: Voter[T, P]): Option[E] =
    error.semigroup.sumOption(trainingData.iterator.map { instance =>
      val id = instance.id
      val timestamp = instance.timestamp
      val features = instance.features

      val predictions = treeMap.iterator
        .filter { case (treeIndex, _) =>
          sampler.includeInValidationSet(id, timestamp, treeIndex)
        }.flatMap { case (_, tree) =>
          tree.targetFor(features)
        }.toVector

      error.create(instance.target, voter.combine(predictions))
    })
}

object Trainer {
  def apply[K: Ordering, V: Ordering, T: Monoid](trainingData: Iterable[Instance[K, V, T]], sampler: Sampler[K])(implicit traversal: AnnotatedTreeTraversal[K, V, T, Unit]): Trainer[K, V, T] = {
    val empty = 0.until(sampler.numTrees).toList.map { i => Tree.singleton[K, V, T](Monoid.zero) }
    Trainer(trainingData, sampler, empty)
  }
}
