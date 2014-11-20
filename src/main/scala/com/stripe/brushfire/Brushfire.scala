package com.stripe.brushfire

import com.twitter.algebird._

case class Instance[V, T](id: String, timestamp: Long, features: Map[String, V], target: T)

object Instance {
  def apply[V](id: String, timestamp: Long, features: Map[String, V], target: Boolean): Instance[V, Map[Boolean, Long]] =
    Instance(id, timestamp, features, Map(target -> 1L))
}

trait Splitter[V, T] {
  type S
  def create(value: V, target: T): S
  def semigroup: Semigroup[S]
  def split(parent: T, stats: S): Iterable[Split[V, T]]
}

trait Split[V, T] {
  def predicates: Iterable[(Predicate[V], T)]
}

trait Evaluator[V, T] {
  def evaluate(split: Split[V, T]): (Split[V, T], Double)
}

trait Sampler {
  def numTrees: Int
  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int): Int
  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int): Boolean
  def includeFeature(name: String, treeIndex: Int, leafIndex: Int): Boolean
}

trait Error[T, E] {
  def semigroup: Semigroup[E]
  def create(actual: T, predicted: Iterable[T]): E
}
