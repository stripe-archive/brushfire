package com.stripe.brushfire.scalding

import com.stripe.brushfire._
import com.twitter.scalding._
import com.twitter.algebird._
import com.twitter.bijection._

import scala.util.Random

abstract class TrainerJob(args: Args) extends ExecutionJob[Unit](args) with Defaults {
  import TDsl._

  def execution = trainer.execution.unit
  def trainer: Trainer[_, _, _, _, _]
}

object TreeSource {
  def apply[K, V, T, A](path: String)(implicit inj: Injection[Tree[K, V, T, A], String]) = {
    implicit val bij = Injection.unsafeToBijection(inj).inverse
    typed.BijectedSourceSink[(Int, String), (Int, Tree[K, V, T, A])](TypedTsv(path))
  }
}

case class Trainer[M, K: Ordering, V, T: Monoid, A](
    @transient trainingDataExecution: Execution[TypedPipe[Instance[M, Map[K, V], T]]],
    @transient samplerExecution: Execution[Sampler[M, K]],
    @transient treeExecution: Execution[TypedPipe[(Int, Tree[K, V, T, A])]],
    @transient unitExecution: Execution[Unit],
    annotator: Annotator[M, A],
    id: M => String,
    reducers: Int) {

  private def stepPath(base: String, n: Int) = base + "/step_%02d".format(n)
  private implicit val annotationMonoid = annotator.monoid

  def execution = Execution.zip(treeExecution, unitExecution).unit

  def flatMapTrees(fn: ((TypedPipe[Instance[M, Map[K, V], T]], Sampler[M, K], Iterable[(Int, Tree[K, V, T, A])])) => Execution[TypedPipe[(Int, Tree[K, V, T, A])]]) = {
    val newExecution = treeExecution
      .flatMap { trees =>
        Execution.zip(trainingDataExecution, samplerExecution, trees.toIterableExecution)
      }.flatMap(fn)
    copy(treeExecution = newExecution)
  }

  def flatMapSampler(fn: ((TypedPipe[Instance[M, Map[K, V], T]], Sampler[M, K])) => Execution[Sampler[M, K]]) = {
    val newExecution = trainingDataExecution.zip(samplerExecution).flatMap(fn)
    copy(samplerExecution = newExecution)
  }

  def tee[U](fn: ((TypedPipe[Instance[M, Map[K, V], T]], Sampler[M, K], Iterable[(Int, Tree[K, V, T, A])])) => Execution[U]): Trainer[M, K, V, T, A] = {
    val newExecution = treeExecution
      .flatMap { trees =>
        Execution.zip(trainingDataExecution, samplerExecution, trees.toIterableExecution)
      }.flatMap(fn)
    copy(unitExecution = unitExecution.zip(newExecution).unit)
  }

  def forceTrainingDataToDisk: Trainer[M, K, V, T, A] = {
    copy(trainingDataExecution = trainingDataExecution.flatMap { _.forceToDiskExecution })
  }

  def load(path: String)(implicit inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    copy(treeExecution = Execution.from(TypedPipe.from(TreeSource(path))))
  }

  /**
   * Update the leaves of the current trees from the training set.
   *
   * The leaves target distributions will be set to the summed distributions of the instances
   * in the training set that would get classified to them. Often used to initialize an empty tree.
   */
  def updateTargets(path: String)(implicit inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        trainingData
          .flatMap { instance =>
            for {
              (treeIndex, tree) <- treeMap
              i <- 1.to(sampler.timesInTrainingSet(instance.metadata, treeIndex)).toList
              leafIndex <- tree.leafIndexFor(instance.features).toList
            } yield (treeIndex, leafIndex) -> (instance.target, annotator.create(instance.metadata))
          }
          .sumByKey
          .map { case ((treeIndex, leafIndex), value) => treeIndex -> Map(leafIndex -> value) }
          .group
          .withReducers(reducers)
          .sum
          .map {
            case (treeIndex, map) => {
              val newTree =
                treeMap(treeIndex)
                  .updateByLeafIndex { index => map.get(index).map { case (t, a) => LeafNode(index, t, a) } }

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
  def expand[S](
      path: String)(implicit splitter: Splitter[V, T],
        evaluator: Evaluator[V, T, A],
        stopper: Stopper[T],
        inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>
        implicit val splitSemigroup = new SplitSemigroup[K, V, T, A]
        implicit val jdSemigroup = splitter.semigroup
        lazy val treeMap = trees.toMap

        val stats =
          trainingData
            .flatMap { instance =>
              lazy val features = instance.features.mapValues { value => (splitter.create(value, instance.target), annotator.create(instance.metadata)) }

              for {
                (treeIndex, tree) <- treeMap
                i <- 1.to(sampler.timesInTrainingSet(instance.metadata, treeIndex)).toList
                leaf <- tree.leafFor(instance.features).toList if stopper.shouldSplit(leaf.target) && stopper.shouldSplitDistributed(leaf.target)
                (feature, stats) <- features if sampler.includeFeature(instance.metadata, feature, treeIndex, leaf.index)
              } yield (treeIndex, leaf.index, feature) -> stats
            }

        val splits =
          stats
            .group
            .sum
            .flatMap {
              case ((treeIndex, leafIndex, feature), (target, annotation)) =>
                treeMap(treeIndex).leafAt(leafIndex).toList.flatMap { leaf =>
                  splitter
                    .split(leaf.target, target, annotation)
                    .map { rawSplit =>
                      val (split, goodness) = evaluator.evaluate(rawSplit)
                      treeIndex -> Map(leafIndex -> (feature, split, goodness))
                    }
                }
            }

        val emptySplits = TypedPipe.from(0.until(sampler.numTrees))
          .map { i => i -> Map[Int, (K, Split[V, T, A], Double)]() }

        (splits ++ emptySplits)
          .group
          .withReducers(reducers)
          .sum
          .map {
            case (treeIndex, map) =>
              val newTree =
                treeMap(treeIndex)
                  .growByLeafIndex { index =>
                    for {
                      (feature, split, _) <- map.get(index).toList
                      (predicate, target, annotation) <- split.predicates
                    } yield (feature, predicate, target, annotation)
                  }

              treeIndex -> newTree
          }.writeThrough(TreeSource(path))
    }
  }

  /** produce an error object from the current trees and the validation set */
  def validate[P, E](error: Error[T, P, E])(fn: ValuePipe[E] => Execution[_])(implicit voter: Voter[T, P]) = {
    tee {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        val err =
          trainingData
            .map { instance =>
              val predictions =
                for {
                  (treeIndex, tree) <- treeMap if sampler.includeInValidationSet(instance.metadata, treeIndex)
                  target <- tree.targetFor(instance.features).toList
                } yield target

              error.create(instance.target, voter.combine(predictions))
            }
            .sum(error.semigroup)
        fn(err)
    }
  }

  /**
   * prune a tree to minimize validation error
   *
   * Construct a Map[Int,T] from the trainingData for each tree, and then transform the trees using the prune method.
   *
   */
  def prune[P, E](path: String, error: Error[T, P, E])(implicit voter: Voter[T, P], inj: Injection[Tree[K, V, T, A], String], ord: Ordering[E]): Trainer[M, K, V, T, A] = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        val newEx = trainingData
          .flatMap { instance =>
            for { // Iterate over any trees for which this instance is a validation instance.
              (treeIndex, tree) <- treeMap if sampler.includeInValidationSet(instance.metadata, treeIndex)
              leafIndex <- tree.leafIndexFor(instance.features).toList // Find the leaf that this instance falls into.
            } yield treeIndex -> Map(leafIndex -> instance.target) // Yield validation instances for each tree, leaf.
          }
          .sumByKey // Make a single Map for each treeIndex. The Map is from leafIndex to validation target.
          .withReducers(reducers)
          .toTypedPipe.map {
            case (treeIndex, map) =>
              // Run the prune method on each tree, passing the validation data:
              (treeIndex, treeMap(treeIndex).prune(map, voter, error))
          }
          .writeThrough(TreeSource(path)) // Write new tree to the given path, and read from this path in the future.
        newEx
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
  def featureImportance[P, E](error: Error[T, P, E])(fn: TypedPipe[(K, E)] => Execution[_])(implicit voter: Voter[T, P]): Trainer[M, K, V, T, A] = {
    lazy val murmur = MurmurHash128(3886428)
    lazy val r = new Random(3886429)
    tee {
      case (trainingData, sampler, trees) =>
        lazy val treeMap = trees.toMap

        val permutedFeatsPipe = trainingData
          .groupRandomly(10).sortBy { instance => murmur(id(instance.metadata)) }.mapValueStream {
            instanceIterator =>
              instanceIterator.sliding(2)
                .flatMap {
                  case List(prevInst, instance) => {
                    val treesForInstance = treeMap.filter {
                      case (treeIndex, tree) => sampler.includeInValidationSet(instance.metadata, treeIndex)
                    }.values

                    val featureToPermute = r.shuffle(prevInst.features).head
                    val permuted = instance.features + featureToPermute

                    val predictions = treesForInstance.flatMap { _.targetFor(permuted) }
                    val instanceErr = error.create(instance.target, voter.combine(predictions))
                    Some((featureToPermute._1, instanceErr))
                  }
                  case _ =>
                    None
                }
          }.values

        implicit val errorSg = error.semigroup
        fn(permutedFeatsPipe.sumByKey.toTypedPipe)
    }
  }

  /** recursively expand multiple times, writing out the new tree at each step */
  def expandTimes(
      base: String, times: Int)(implicit splitter: Splitter[V, T],
        evaluator: Evaluator[V, T, A],
        stopper: Stopper[T],
        inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    updateTargets(stepPath(base, 0))
      .expandFrom(base, 1, times)
  }

  def expandFrom(
      base: String,
      step: Int,
      to: Int)(implicit splitter: Splitter[V, T],
        evaluator: Evaluator[V, T, A],
        stopper: Stopper[T],
        inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    if (step > to)
      this
    else {
      expand(stepPath(base, step))(splitter, evaluator, stopper, inj)
        .expandFrom(base, step + 1, to)
    }
  }

  def expandInMemory(
      path: String,
      times: Int)(implicit splitter: Splitter[V, T],
        evaluator: Evaluator[V, T, A],
        stopper: Stopper[T],
        annotator: Annotator[M, A],
        inj: Injection[Tree[K, V, T, A], String]): Trainer[M, K, V, T, A] = {
    flatMapTrees {
      case (trainingData, sampler, trees) =>

        lazy val treeMap = trees.toMap
        lazy val r = new Random(123)
        implicit val annotationMonoid = annotator.monoid

        val expansions =
          trainingData
            .flatMap { instance =>
              for {
                (treeIndex, tree) <- treeMap
                i <- 1.to(sampler.timesInTrainingSet(instance.metadata, treeIndex)).toList
                leaf <- tree.leafFor(instance.features).toList if stopper.shouldSplit(leaf.target) && (r.nextDouble < stopper.samplingRateToSplitLocally(leaf.target))
              } yield (treeIndex, leaf.index) -> instance
            }
            .group
            .forceToReducers
            .toList
            .map {
              case ((treeIndex, leafIndex), instances) =>
                val (target, annotation) = Monoid.sum(instances.map { i => (i.target, annotator.create(i.metadata)) })
                val leaf = LeafNode[K, V, T, A](0, target, annotation)
                val expanded = Tree.expand(times, leaf, splitter, evaluator, stopper, annotator, instances)
                treeIndex -> List(leafIndex -> expanded)
            }

        val emptyExpansions = TypedPipe.from(0.until(sampler.numTrees))
          .map { i => i -> List[(Int, Node[K, V, T, A])]() }

        (expansions ++ emptyExpansions)
          .group
          .withReducers(reducers)
          .sum
          .map {
            case (treeIndex, list) =>

              val map = list.toMap

              val newTree =
                treeMap(treeIndex)
                  .updateByLeafIndex { index => map.get(index) }

              treeIndex -> newTree
          }.writeThrough(TreeSource(path))
    }
  }

  /** add out of time validation */
  def outOfTime(quantile: Double = 0.8)(implicit eq: M =:= DefaultMetadata): Trainer[M, K, V, T, A] = {
    flatMapSampler {
      case (trainingData, sampler) =>

        implicit val qtree = new QTreeSemigroup[Long](6)

        trainingData
          .map { instance => QTree(instance.metadata.timestamp) }
          .sum
          .map { q => q.quantileBounds(quantile)._2.toLong }
          .toIterableExecution
          .map { thresholds => OutOfTimeSampler[M, K](timestamp = _.timestamp, sampler, thresholds.head) }
    }
  }
}

object Trainer {
  val MaxReducers = 20

  def apply[M, K: Ordering, V, T: Monoid, A](
      trainingData: TypedPipe[Instance[M, Map[K, V], T]],
      annotator: Annotator[M, A],
      sampler: Sampler[M, K],
      id: M => String): Trainer[M, K, V, T, A] = {
    implicit val annotationSemigroup = annotator.monoid
    val empty = 0.until(sampler.numTrees).map { treeIndex => (treeIndex, Tree.singleton[K, V, T, A](Monoid.zero[T], annotator.monoid.zero)) }
    Trainer(
      Execution.from(trainingData),
      Execution.from(sampler),
      Execution.from(TypedPipe.from(empty)),
      Execution.from(()),
      annotator,
      id,
      sampler.numTrees.min(MaxReducers))
  }
}

class SplitSemigroup[K, V, T, A] extends Semigroup[(K, Split[V, T, A], Double)] {
  def plus(a: (K, Split[V, T, A], Double), b: (K, Split[V, T, A], Double)): (K, Split[V, T, A], Double) = {
    if (a._3 > b._3)
      a
    else
      b
  }
}
