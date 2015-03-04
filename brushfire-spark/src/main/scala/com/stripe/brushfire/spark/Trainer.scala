package com.stripe.brushfire
package spark

import com.twitter.algebird._
import com.twitter.algebird.Operators._

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.reflect.{ classTag, ClassTag }
import scala.util.Random

case class Trainer[K: Ordering, V, T: Monoid: ClassTag](
    trainingData: RDD[Instance[K, V, T]],
    sampler: Sampler[K],
    trees: RDD[(Int, Tree[K, V, T])]) {
  import Trainer.ExtraRDDOps

  private val context: SparkContext = trainingData.context

  /**
   * Update the leaves of the current trees from the training set.
   *
   * The leaves target distributions will be set to the summed distributions of the instances
   * in the training set that would get classified to them. Often used to initialize an empty tree.
   */
  def updateTargets: Trainer[K, V, T] = {
    type LeafId = (Int, Int)

    val treeMap: scala.collection.Map[Int, Tree[K, V, T]] = trees.collectAsMap()

    val collectLeaves: Instance[K, V, T] => Iterable[(LeafId, T)] = { instance =>
      for {
        (treeIndex, tree) <- treeMap
        repetition = sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)
        i <- 1 to repetition
        leafIndex <- tree.leafIndexFor(instance.features).toList
      } yield {
        (treeIndex, leafIndex) -> instance.target
      }
    }

    val newTrees = trainingData
      .flatMap(collectLeaves)
      .sumByKey
      .map {
        case ((treeIndex, leafIndex), target) =>
          treeIndex -> Map(leafIndex -> target)
      }
      .sumByKey
      .map {
        case (treeIndex, leafTargets) =>
          val newTree =
            treeMap(treeIndex).updateByLeafIndex { index =>
              leafTargets.get(index).map(LeafNode(index, _))
            }

          treeIndex -> newTree
      }
      .cache()

    copy(trees = newTrees)
  }

  def expandInMemory(times: Int)(implicit evaluator: Evaluator[V, T], splitter: Splitter[V, T], stopper: Stopper[T]): Trainer[K, V, T] = {
    type LeafId = (Int, Int)

    val treeMap: scala.collection.Map[Int, Tree[K, V, T]] = trees.collectAsMap()
    val rng: Random = new Random(1234)

    val sampleInstances: Instance[K, V, T] => Iterable[(LeafId, Instance[K, V, T])] = { instance =>
      for {
        (treeIndex, tree) <- treeMap
        repetition = sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)
        i <- 1 to repetition
        leaf <- tree.leafFor(instance.features).toList
        if stopper.shouldSplit(leaf.target) && rng.nextDouble < stopper.samplingRateToSplitLocally(leaf.target)
      } yield {
        (treeIndex, leaf.index) -> instance
      }
    }

    val emptyExpansions = context
      .parallelize(0 until sampler.numTrees)
      .map { _ -> List[(Int, Node[K, V, T])]() }

    val newTrees = trainingData
      .flatMap(sampleInstances)
      .groupByKey()
      .map {
        case ((treeIndex, leafIndex), instances) =>
          val target = Monoid.sum(instances.map { _.target })
          val leaf = LeafNode[K, V, T](0, target)
          val expanded = Tree.expand(times, leaf, splitter, evaluator, stopper, instances)
          treeIndex -> List(leafIndex -> expanded)
      }
      .union(emptyExpansions)
      .sumByKey
      .map {
        case (treeIndex, leafExpansions) =>
          val expansions = leafExpansions.toMap
          val newTree = treeMap(treeIndex)
            .updateByLeafIndex(expansions.get(_))
          treeIndex -> newTree
      }
      .cache()

    copy(trees = newTrees)
  }

  def expandTimes(times: Int)(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]): Trainer[K, V, T] = {
    def loop(trainer: Trainer[K, V, T], i: Int): Trainer[K, V, T] =
      if (i > 0) loop(trainer.expand, i - 1)
      else trainer

    loop(updateTargets, times)
  }

  /**
   * Grow the trees by splitting all the leaf nodes as the stopper allows.
   */
  private def expand(implicit splitter: Splitter[V, T], evaluator: Evaluator[V, T], stopper: Stopper[T]): Trainer[K, V, T] = {
    // Our bucket has a tree index, leaf index, and feature.
    type Bucket = (Int, Int, K)

    // A scored split for a particular feature.
    type ScoredSplit = (K, Split[V, T], Double)

    implicit object ScoredSplitSemigroup extends Semigroup[ScoredSplit] {
      def plus(a: ScoredSplit, b: ScoredSplit) =
        if (b._3 > a._3) b else a
    }

    val treeMap: scala.collection.Map[Int, Tree[K, V, T]] = trees.collectAsMap()

    val collectFeatures: Instance[K, V, T] => Iterable[(Bucket, splitter.S)] = { instance =>
      val features = instance.features.mapValues(splitter.create(_, instance.target))
      for {
        (treeIndex, tree) <- treeMap
        repetition = sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)
        i <- 1 to repetition
        leaf <- tree.leafFor(instance.features).toList
        if stopper.shouldSplit(leaf.target) && stopper.shouldSplitDistributed(leaf.target)
        (feature, stats) <- features
        if sampler.includeFeature(feature, treeIndex, leaf.index)
      } yield {
        (treeIndex, leaf.index, feature) -> stats
      }
    }

    val split: (Bucket, splitter.S) => Iterable[(Int, Map[Int, ScoredSplit])] = { (bucket, stats) =>
      val (treeIndex, leafIndex, feature) = bucket
      for {
        leaf <- treeMap(treeIndex).leafAt(leafIndex).toList
        rawSplit <- splitter.split(leaf.target, stats)
      } yield {
        val (split, goodness) = evaluator.evaluate(rawSplit)
        treeIndex -> Map(leafIndex -> (feature, split, goodness))
      }
    }

    val emptySplits: RDD[(Int, Map[Int, ScoredSplit])] =
      context.parallelize(Seq.tabulate(sampler.numTrees)(_ -> Map.empty[Int, ScoredSplit]))

    val growTree: (Int, Map[Int, ScoredSplit]) => (Int, Tree[K, V, T]) = { (treeIndex, leafSplits) =>
      val newTree =
        treeMap(treeIndex)
          .growByLeafIndex { index =>
            for {
              (feature, split, _) <- leafSplits.get(index).toList
              (predicate, target) <- split.predicates
            } yield {
              (feature, predicate, target)
            }
          }
      treeIndex -> newTree
    }

    // Ugh. We could also wrap splitter.S in some box...
    implicit val existentialClassTag: ClassTag[splitter.S] =
      scala.reflect.classTag[AnyRef].asInstanceOf[ClassTag[splitter.S]]

    val newTrees = trainingData
      .flatMap(collectFeatures)
      .reduceByKey(splitter.semigroup.plus(_, _))
      .flatMap(split.tupled)
      .union(emptySplits)
      .reduceByKey(_ + _)
      .map(growTree.tupled)
      .cache()

    copy(trees = newTrees)
  }
}

object Trainer {
  val MaxParallelism = 20

  def apply[K: Ordering, V, T: Monoid: ClassTag](trainingData: RDD[Instance[K, V, T]], sampler: Sampler[K]): Trainer[K, V, T] = {
    val sc = trainingData.context
    val initialTrees = Vector.tabulate(sampler.numTrees) { _ -> Tree.empty[K, V, T](Monoid.zero) }
    val parallelism = scala.math.min(MaxParallelism, sampler.numTrees)
    Trainer(
      trainingData,
      sampler,
      sc.parallelize(initialTrees, parallelism))
  }

  implicit class ExtraRDDOps[A](val rdd: RDD[A]) extends AnyVal {
    def sumByKey[K, V](implicit ev: A <:< (K, V), ctK: ClassTag[K], ctV: ClassTag[V], V: Monoid[V]): RDD[(K, V)] =
      rdd.asInstanceOf[RDD[(K, V)]].foldByKey(V.zero)(V.plus)
  }
}
