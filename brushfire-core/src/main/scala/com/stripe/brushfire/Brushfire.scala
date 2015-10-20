package com.stripe.brushfire

import com.twitter.algebird._

/**
 * Represents a single instance of training data.
 *
 * @tparam M instance metadata
 * @tparam F features
 * @tparam T target distribution
 *
 * @constructor create a new instance
 * @param metadata metadata about this instance
 * @param features a map of named features that make up this instance
 * @param target a distribution of predictions or labels for this instance
 */
case class Instance[M, F, T](metadata: M, features: F, target: T)

object Instance {
  def apply[K, V](id: String, timestamp: Long, features: Map[K, V], target: Boolean): Instance[DefaultMetadata, Map[K, V], Map[Boolean, Long]] =
    Instance(DefaultMetadata(id, timestamp), features, Map(target -> 1L))
}

case class DefaultMetadata(id: String, timestamp: Long)

/**
 * Produces candidate splits from the instances at a leaf node.
 * @tparam V feature values
 * @tparam T target distributions
 */
trait Splitter[V, T] {
  /** the type of a representation of a joint distribution of feature values and predictions */
  type S

  /** return a new joint distribution from a value and a target distribution */
  def create(value: V, target: T): S

  /** semigroup to sum up joint distributions */
  def semigroup: Semigroup[S]

  /**
   * generate candidate splits
   * @param parent the parent node's target distribution
   * @param stats the joint distribution to split
   * @param annotation an annotation that must be preserved in each split candidate
   */
  def split[A](parent: T, stats: S, annotation: A): Iterable[Split[V, T, A]]
}

/** Candidate split for a tree node */
trait Split[V, T, A] {
  def predicates: Iterable[(Predicate[V], T, A)]
}

/** Evaluates the goodness of a candidate split */
trait Evaluator[V, T, A] {
  /** returns a (possibly transformed) version of the input split, and a numeric goodness score */
  def evaluate(split: Split[V, T, A]): (Split[V, T, A], Double)
}

/** Provides stopping conditions which guide when splits will be attempted */
trait Stopper[T] {
  def shouldSplit(target: T): Boolean
  def shouldSplitDistributed(target: T): Boolean
  def samplingRateToSplitLocally(target: T): Double
}

/** Allocates instances and features to trees and training or validation sets */
trait Sampler[-M, -K] {
  /** returns number of trees to train */
  def numTrees: Int

  /** returns how many copies (0 to n) of an instance to train a given tree with */
  def timesInTrainingSet(metadata: M, treeIndex: Int): Int

  /** returns whether to use an instance to validate a given tree */
  def includeInValidationSet(metadata: M, treeIndex: Int): Boolean

  /** returns whether to consider a feature when splitting a given leaf */
  def includeFeature(metadata: M, key: K, treeIndex: Int, leafIndex: Int): Boolean
}

/** Computes some kind of error by comparing the trees' predictions to the validation set */
trait Error[T, P, E] {
  /** semigroup to sum up error values */
  def semigroup: Semigroup[E]

  /**
   * create an single component of the error value
   *
   * @param actual the actual target distribution from the validation set
   * @param predicted the set of predicted distributions from the trees
   */
  def create(actual: T, predicted: P): E
}

/** Converts instance metadata to tree annotations */
trait Annotator[M, A] {
  def monoid: Monoid[A]

  def create(metadata: M): A
}
