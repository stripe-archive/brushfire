package com.stripe.brushfire

import com.twitter.algebird._

/** Represents a single instance of training data.
*
* @tparam K feature names
* @tparam V feature values
* @tparam T target distribution
*
* @constructor create a new instance
* @param id an identiier unique to this instance
* @param timestamp the time this instance was observed
* @param features a map of named features that make up this instance
* @param target a distribution of predictions or labels for this instance
**/
case class Instance[K, V, T](id: String, timestamp: Long, features: Map[K, V], target: T)

object Instance {
  def apply[K, V](id: String, timestamp: Long, features: Map[K, V], target: Boolean): Instance[K, V, Map[Boolean, Long]] =
    Instance(id, timestamp, features, Map(target -> 1L))
}

/** Produces candidate splits from the instances at a leaf node.
* @tparam V feature values
* @tparam T target distrubutions
**/
trait Splitter[V, T] {
  /** the type of a representation of a joint distribution of feature values and predictions **/
  type S

  /** return a new joint distribution from a value and a target distribution **/
  def create(value: V, target: T): S

  /** semigroup to sum up joint distributions **/
  def semigroup: Semigroup[S]

  /** return candidate splits given a joint distribution and the parent node's target distrubution **/
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
