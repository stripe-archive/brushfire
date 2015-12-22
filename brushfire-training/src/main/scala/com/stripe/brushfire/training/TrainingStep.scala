package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

trait TrainingStep[K,V,T] {

  type K1
  type V1
  type V2

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]): Seq[((Int,Int,K1), V1)]
  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K1, v1: V1): Traversable[V2]
  def update(tree: Tree[K,V,T], map: Map[Int,V2]): Tree[K,V,T]

  def semigroup1: Semigroup[V1]
  def semigroup2: Semigroup[V2]
  def ordering: Ordering[K1]
}

case class UpdateTargets[K,V,T](sampler: Sampler[K])(implicit val semigroup1: Semigroup[T])
  extends TrainingStep[K,V,T] {

  type K1 = Unit
  type V1 = T
  type V2 = T

  val semigroup2 = semigroup1
  val ordering = implicitly[Ordering[Unit]]

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]) = {
     for (
        (treeIndex, tree) <- trees.toList;
        i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
        leafIndex <- tree.leafIndexFor(instance.features).toList
      ) yield (treeIndex, leafIndex, ()) -> instance.target
  }

  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K1, v1: T) = List(v1)

  def update(tree: Tree[K,V,T], map: Map[Int,T]) = {
    tree.updateByLeafIndex { index => map.get(index).map { t => LeafNode(index, t) } }
  }
}

case class Expand[K,V,T](sampler: Sampler[K], stopper: Stopper[T], splitter: Splitter[V, T], evaluator: Evaluator[V, T])(implicit val ordering: Ordering[K])
  extends TrainingStep[K,V,T] {

  type K1 = K
  type V1 = splitter.S
  type V2 = (K, Split[V, T], Double)

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]) = {
    lazy val features = instance.features.mapValues { value => splitter.create(value, instance.target) }

    for
      ((treeIndex, tree) <- trees.toList;
      i <- 1.to(sampler.timesInTrainingSet(instance.id, instance.timestamp, treeIndex)).toList;
      leaf <- tree.leafFor(instance.features).toList if stopper.shouldSplit(leaf.target);
      (feature, stats) <- features if (sampler.includeFeature(feature, treeIndex, leaf.index)))
        yield (treeIndex, leaf.index, feature) -> stats
  }

  def lift(tree: Tree[K,V,T], leafIndex: Int, key: K, v1: V1) = {
    tree.leafAt(leafIndex).toList.flatMap { leaf =>
      splitter
        .split(leaf.target, v1)
        .map { rawSplit =>
          val (split, goodness) = evaluator.evaluate(rawSplit)
          (key, split, goodness)
        }
    }
  }

  def update(tree: Tree[K,V,T], map: Map[Int,V2]) = {
    tree.growByLeafIndex { index =>
      for (
        (feature, split, _) <- map.get(index).toList;
        (predicate, target) <- split.predicates
      ) yield (feature, predicate, target, ())
    }
  }

  val semigroup1 = splitter.semigroup
  val semigroup2 = new Semigroup[V2] {
    def plus(a: V2, b: V2) = {
      if (a._3 > b._3) a else b
    }
  }
}
