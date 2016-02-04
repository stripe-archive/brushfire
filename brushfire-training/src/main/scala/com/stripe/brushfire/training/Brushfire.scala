package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

/**
 * Represents a single instance of training data.
 *
 * @tparam K feature names
 * @tparam V feature values
 * @tparam T target distribution
 *
 * @constructor create a new instance
 * @param id an identifier unique to this instance
 * @param timestamp the time this instance was observed
 * @param features a map of named features that make up this instance
 * @param target a distribution of predictions or labels for this instance
 */
case class Instance[K, V, T](id: String, timestamp: Long, features: Map[K, V], target: T)

object Instance {
  def apply[K, V](id: String, timestamp: Long, features: Map[K, V], target: Boolean): Instance[K, V, Map[Boolean, Long]] =
    Instance(id, timestamp, features, Map(target -> 1L))
}

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

  /** return candidate splits given a joint distribution and the parent node's target distrubution */
  def split(parent: T, stats: S): Iterable[Split[V, T]]
}

/** Candidate split for a tree node */
case class Split[V, T](predicate: Predicate[V], leftDistribution: T, rightDistribution: T) {

  /**
   * Given a feature key, create a SplitNode from this Split.
   *
   * Note that the leaves of this node will likely need to be
   * renumbered if this node is put into a larger tree.
   */
  def createSplitNode[K](feature: K): SplitNode[K, V, T, Unit] =
    SplitNode(feature, predicate, LeafNode(0, leftDistribution), LeafNode(1, rightDistribution))
}

trait Evaluator[T] {
  /** returns an overall numeric training error for a tree or split, or None for infinite/unacceptable error */
  def trainingError(leaves: Iterable[T]): Option[Double]
}

/** Provides stopping conditions which guide when splits will be attempted */
trait Stopper[T] {
  def shouldSplit(target: T): Boolean
  def shouldSplitDistributed(target: T): Boolean
  def samplingRateToSplitLocally(target: T): Double
}

/** Allocates instances and features to trees and training or validation sets */
trait Sampler[-K] {
  /** returns number of trees to train */
  def numTrees: Int

  /** returns how many copies (0 to n) of an instance to train a given tree with */
  def timesInTrainingSet(id: String, timestamp: Long, treeIndex: Int): Int

  /** returns whether to use an instance to validate a given tree */
  def includeInValidationSet(id: String, timestamp: Long, treeIndex: Int): Boolean

  /** returns whether to consider a feature when splitting a given leaf */
  def includeFeature(key: K, treeIndex: Int, leafIndex: Int): Boolean
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
