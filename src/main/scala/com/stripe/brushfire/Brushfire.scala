package com.stripe.brushfire

import com.twitter.algebird._

case class Instance[K, V, T](id: String, timestamp: Long, features: Map[K, V], target: T)

object Instance {
  def apply[K, V](id: String, timestamp: Long, features: Map[K, V], target: Boolean): Instance[K, V, Map[Boolean, Long]] =
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

trait Sampler[-K] {
  def numTrees: Int
  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int): Int
  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int): Boolean
  def includeFeature(key: K, treeIndex: Int, leafIndex: Int): Boolean
}

trait Error[T, E] {
  def semigroup: Semigroup[E]
  def create(actual: T, predicted: Iterable[T]): E
}
