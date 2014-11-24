package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.bijection._

import scala.util.Random

abstract class TrainerJob(args: Args) extends ExecutionJob[Unit](args) with Defaults with JsonInjections {
  import TDsl._

  def execution = trainer.execution.unit
  def trainer: Trainer[_, _, _]
}

object TreeSource {
  def apply[K, V, T](path: String)(implicit inj: Injection[Tree[K, V, T], String]) = {
    implicit val bij = Injection.unsafeToBijection(inj).inverse
    typed.BijectedSourceSink[(Int, String), (Int, Tree[K, V, T])](TypedTsv(path))
  }
}

case class Trainer[K: Ordering, V, T: Monoid](
    trainingDataExecution: Execution[TypedPipe[Instance[K, V, T]]],
    samplerExecution: Execution[Sampler[K]],
    treeExecution: Execution[TypedPipe[(Int, Tree[K, V, T])]],
    unitExecution: Execution[Unit],
    reducers: Int) {

  private def stepPath(base: String, n: Int) = base + "/step_%02d".format(n)

  def execution = Execution.zip(treeExecution, unitExecution).unit

  def flatMapTrees(fn: ((TypedPipe[Instance[K, V, T]], Sampler[K], Iterable[(Int, Tree[K, V, T])])) => Execution[TypedPipe[(Int, Tree[K, V, T])]]) = {
    val newExecution = treeExecution
      .flatMap { trees =>
        Execution.zip(trainingDataExecution, samplerExecution, trees.toIterableExecution)
      }.flatMap(fn)
    copy(treeExecution = newExecution)
  }

  def flatMapSampler(fn: ((TypedPipe[Instance[K, V, T]], Sampler[K])) => Execution[Sampler[K]]) = {
    val newExecution = trainingDataExecution.zip(samplerExecution).flatMap(fn)
    copy(samplerExecution = newExecution)
  }

  def tee[A](fn: ((TypedPipe[Instance[K, V, T]], Sampler[K], Iterable[(Int, Tree[K, V, T])])) => Execution[A]): Trainer[K, V, T] = {
    val newExecution = treeExecution
      .flatMap { trees =>
        Execution.zip(trainingDataExecution, samplerExecution, trees.toIterableExecution)
      }.flatMap(fn)
    copy(unitExecution = unitExecution.zip(newExecution).unit)
  }

  def load(path: String)(implicit inj: Injection[Tree[K, V, T], String]): Trainer[K, V, T] = {
    copy(treeExecution = Execution.from(TypedPipe.from(TreeSource(path))))
  }

  /**
   * Update the leaves of the current trees from the training set.
   *
   * The leaves target distributions will be set to the summed distributions of the instances
   * in the training set that would get classified to them. Often used to initialize an empty tree.
   */
  def updateTargets(path: String)(implicit inj: Injection[Tree[K, V, T], String]): Trainer[K, V, T] = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        trainingData
          .flatMap { instance =>
            for (
              (treeIndex, tree) <- treeMap;
              i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
              leafIndex <- tree.leafIndexFor(instance.features).toList
            ) yield (treeIndex, leafIndex) -> instance.target
          }
          .sumByKey
          .map { case ((treeIndex, leafIndex), target) => treeIndex -> Map(leafIndex -> target) }
          .group
          .withReducers(reducers)
          .sum
          .map {
            case (treeIndex, map) => {
              val newTree =
                treeMap(treeIndex)
                  .updateByLeafIndex { index => map.get(index) }

              treeIndex -> newTree
            }
          }.writeThrough(TreeSource(path))
    }
  }

  /**
   * expand each tree by one level, by attempting to split every leaf.
   * @param path where to save the new tree
   * @param splitter the splitter to use to generate candidate splits for each leaf
   * @param evaluator the evaluator to use to decide which split to use for each leaf
   */
  def expand[S](path: String)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[K, V, T], String]) = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>
        implicit val splitSemigroup = new SplitSemigroup[K, V, T]
        implicit val jdSemigroup = splitter.semigroup
        lazy val treeMap = trees.toMap

        trainingData
          .flatMap { instance =>
            for (
              (treeIndex, tree) <- treeMap;
              i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
              leafIndex <- tree.leafIndexFor(instance.features).toList;
              (feature, value) <- instance.features if (sampler.includeFeature(feature, treeIndex, leafIndex))
            ) yield (treeIndex, leafIndex, feature) ->
              splitter.create(value, instance.target)
          }
          .group
          .sum
          .flatMap {
            case ((treeIndex, leafIndex, feature), target) =>
              treeMap(treeIndex).leafAt(leafIndex).toList.flatMap { leaf =>
                splitter
                  .split(leaf.target, target)
                  .map { rawSplit =>
                    val (split, goodness) = evaluator.evaluate(rawSplit)
                    treeIndex -> Map(leafIndex -> (feature, split, goodness))
                  }
              }
          }
          .group
          .withReducers(reducers)
          .sum
          .map {
            case (treeIndex, map) =>
              val newTree =
                treeMap(treeIndex)
                  .growByLeafIndex { index =>
                    for (
                      (feature, split, _) <- map.get(index).toList;
                      (predicate, target) <- split.predicates
                    ) yield (feature, predicate, target)
                  }

              treeIndex -> newTree
          }.writeThrough(TreeSource(path))
    }
  }

  /** produce an error object from the current trees and the validation set */
  def validate[E](error: Error[T, E])(fn: ValuePipe[E] => Execution[_]) = {
    tee {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        val err =
          trainingData
            .flatMap { instance =>
              val predictions =
                for (
                  (treeIndex, tree) <- treeMap if sampler.includeInValidationSet(instance.id, instance.timestamp, treeIndex);
                  target <- tree.targetFor(instance.features).toList
                ) yield target

              if (predictions.isEmpty)
                None
              else
                Some(error.create(instance.target, predictions))
            }
            .sum(error.semigroup)
        fn(err)
    }
  }

  /**
   *  featureImportance should: shuffle data randomly (group on something random then sort on something random?),
   * then stream through and have each instance pick one feature value at random to pass on to the following instance.
   * then group by permuted feature and compare error.
   * @param error
   * @tparam E
   * @return
   */
  def featureImportance[E](error: Error[T, E])(fn: TypedPipe[(K, E)] => Execution[_]) = {
    lazy val r = new Random(123)
    tee {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        val permutedFeatsPipe = trainingData.groupRandomly(10).sortBy { _ => r.nextDouble() }.mapValueStream {
          instanceIterator =>
            instanceIterator.sliding(2)
              .flatMap {
                case List(prevInst, instance) =>
                  val treesForInstance = treeMap.filter {
                    case (treeIndex, tree) => sampler.includeInValidationSet(instance.id, instance.timestamp, treeIndex)
                  }.values

                  treesForInstance.map { tree =>
                    val featureToPermute = r.shuffle(prevInst.features).head
                    val permuted = instance.features + featureToPermute
                    val instanceErr = error.create(instance.target, tree.targetFor(permuted))
                    (featureToPermute._1, instanceErr)
                  }
                case _ =>
                  Nil
              }
        }.values

        val summed = permutedFeatsPipe.groupBy(_._1)
          .mapValues(_._2)
          .sum(error.semigroup)
        fn(summed.toTypedPipe)
    }
  }

  /** recursively expand multiple times, writing out the new tree at each step */
  def expandTimes[S](base: String, times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[K, V, T], String]) = {
    updateTargets(stepPath(base, 0))
      .expandFrom(base, 1, times)
  }

  def expandFrom[S](base: String, step: Int, to: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[K, V, T], String]): Trainer[K, V, T] = {
    if (step > to)
      this
    else {
      expand(stepPath(base, step))(splitter, evaluator, inj)
        .expandFrom(base, step + 1, to)
    }
  }

  /** add out of time validation */
  def outOfTime(quantile: Double = 0.8) = {
    flatMapSampler {
      case (trainingData, sampler) =>

        implicit val qtree = new QTreeSemigroup[Long](6)

        trainingData
          .map { instance => QTree(instance.timestamp) }
          .sum
          .map { q => q.quantileBounds(quantile)._2.toLong }
          .toIterableExecution
          .map { thresholds => OutOfTimeSampler(sampler, thresholds.head) }
    }
  }
}

object Trainer {
  val MaxReducers = 20

  def apply[K: Ordering, V, T: Monoid](trainingData: TypedPipe[Instance[K, V, T]], sampler: Sampler[K]): Trainer[K, V, T] = {
    val empty = 0.until(sampler.numTrees).map { treeIndex => (treeIndex, Tree.empty[K, V, T](Monoid.zero)) }
    Trainer(
      trainingData.forceToDiskExecution,
      Execution.from(sampler),
      Execution.from(TypedPipe.from(empty)),
      Execution.from(()),
      sampler.numTrees.min(MaxReducers))
  }
}

class SplitSemigroup[K, V, T] extends Semigroup[(K, Split[V, T], Double)] {
  def plus(a: (K, Split[V, T], Double), b: (K, Split[V, T], Double)) = {
    if (a._3 > b._3)
      a
    else
      b
  }
}
