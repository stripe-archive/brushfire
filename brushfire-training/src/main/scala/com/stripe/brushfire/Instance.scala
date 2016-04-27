package com.stripe.brushfire

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
