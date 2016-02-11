package com.stripe.brushfire
package features

// 
sealed trait FeatureType
object FeatureType {
  case object Ordinal extends FeatureType
  case object Nominal extends FeatureType
  case object Continuous extends FeatureType
  case object Sparse extends FeatureType
}

// Deprecated.
case class FeatureMapping[A](
  mapping: Map[String, (FeatureType, A => FeatureValue)]
) {
  def featureType(key: String): FeatureType = mapping(key)._1
  def extract(key: String, value: A): FeatureValue = mapping(key)._2.apply(value)
}

