package com.stripe.brushfire
package features

import scala.util.Try

trait FeatureEncoder[K, +V] {
  def encode(value: Map[String, FValue]): Try[Map[K, V]]
}

trait FeatureEncoding[K, V, T] {
  def splitter: Splitter[V, T]
  def encoder: FeatureEncoder[K, V]
}
