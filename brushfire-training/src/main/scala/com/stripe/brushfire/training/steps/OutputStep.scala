package com.stripe.brushfire.training.steps

import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.algebird._

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
