package com.stripe.brushfire.local

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


trait OutputStep[K,V,T,O] {
  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]): Seq[O]
  def semigroup: Semigroup[O]
}

case class Validate[K,V,T:Semigroup,P,E](sampler: Sampler[K], error: Error[T, P,E], voter: Voter[T, P])
  extends OutputStep[K,V,T,E] {

  def prepare(trees: Map[Int, Tree[K,V,T]], instance: Instance[K,V,T]) = {
    val predictions =
      for (
        (treeIndex, tree) <- trees
          if sampler.includeInValidationSet(instance.id, instance.timestamp, treeIndex);
        target <- tree.targetFor(instance.features).toList
      ) yield target

    List(error.create(instance.target, voter.combine(predictions)))
  }

  val semigroup = error.semigroup
}

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
