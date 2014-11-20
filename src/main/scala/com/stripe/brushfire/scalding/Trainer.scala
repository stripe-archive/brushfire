package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.bijection._

abstract class TrainerJob(args: Args) extends ExecutionJob[Unit](args) with Defaults with JsonInjections {
  import TDsl._

  def execution = trainer.execution.unit
  def trainer: Trainer[_, _]
}

object TreeSource {
  def apply[V, T](path: String)(implicit inj: Injection[Tree[V, T], String]) = {
    implicit val bij = Injection.unsafeToBijection(inj).inverse
    typed.BijectedSourceSink[(Int, String), (Int, Tree[V, T])](TypedTsv(path))
  }
}

case class TrainerState[V, T](sampler: Sampler, trees: TypedPipe[(Int, Tree[V, T])])

case class Trainer[V, T: Monoid](trainingData: TypedPipe[Instance[V, T]], reducers: Int, execution: Execution[TrainerState[V, T]]) {

  private def stepPath(base: String, n: Int) = base + "/step_%02d".format(n)

  def flatMapTrees(fn: ((Sampler, Iterable[(Int, Tree[V, T])])) => Execution[TypedPipe[(Int, Tree[V, T])]]) = {
    val newExecution = execution
      .flatMap { state =>
        Execution.from(state.sampler).zip(state.trees.toIterableExecution)
      }.flatMap(fn)
      .zip(execution.map { _.sampler })
      .map { case (t, s) => TrainerState(s, t) }
    copy(execution = newExecution)
  }

  def flatMapSampler(fn: Sampler => Execution[Sampler]) = {
    val newExecution = execution
      .flatMap { state => fn(state.sampler) }
      .zip(execution.map { _.trees })
      .map { case (s, t) => TrainerState(s, t) }
    copy(execution = newExecution)
  }

  def load(path: String)(implicit inj: Injection[Tree[V, T], String]): Trainer[V, T] = {
    flatMapTrees { case (sampler, _) => Execution.from(TypedPipe.from(TreeSource(path))) }
  }

  def updateTargets(path: String)(implicit inj: Injection[Tree[V, T], String]): Trainer[V, T] = {
    flatMapTrees {
      case (sampler, trees) =>
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

  def expand[S](path: String)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[V, T], String]) = {
    flatMapTrees {
      case (sampler, trees) =>
        implicit val splitSemigroup = new SplitSemigroup[V, T]
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

  def validate[E](error: Error[T, E])(fn: ValuePipe[E] => Execution[_]) = {
    flatMapTrees {
      case (sampler, trees) =>
        lazy val treeMap = trees.toMap

        val e = trainingData
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

        fn(e).unit.flatMap { u => execution.map { _.trees } }
    }
  }

  def expandTimes[S](base: String, times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[V, T], String]) = {
    updateTargets(stepPath(base, 0))
      .expandFrom(base, 1, times)
  }

  def expandFrom[S](base: String, step: Int, to: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], inj: Injection[Tree[V, T], String]): Trainer[V, T] = {
    if (step > to)
      this
    else {
      expand(stepPath(base, step))(splitter, evaluator, inj)
        .expandFrom(base, step + 1, to)
    }
  }

  def outOfTime(quantile: Double = 0.8) = {
    flatMapSampler { sampler =>
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

  def apply[V, T: Monoid](trainingData: TypedPipe[Instance[V, T]], sampler: Sampler): Trainer[V, T] = {
    val empty = 0.until(sampler.numTrees).map { treeIndex => (treeIndex, Tree.empty[V, T](Monoid.zero)) }
    Trainer(trainingData, sampler.numTrees.min(MaxReducers), Execution.from(TrainerState(sampler, TypedPipe.from(empty))))
  }
}

class SplitSemigroup[V, T] extends Semigroup[(String, Split[V, T], Double)] {
  def plus(a: (String, Split[V, T], Double), b: (String, Split[V, T], Double)) = {
    if (a._3 > b._3)
      a
    else
      b
  }
}
