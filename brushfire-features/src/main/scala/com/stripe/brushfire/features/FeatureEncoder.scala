package com.stripe.brushfire
package features

// sealed trait FeatureValue
// object FeatureValue {
//   case class FString(value: String) extends FeatureValue
//   case class FDouble(value: Double) extends FeatureValue
//   sealed abstract class FBoolean(val value: Boolean) extends FeatureValue
//   case object FTrue extends FBoolean(true)
//   case object FFalse extends FBoolean(false)
//   case object FNull extends FeatureValue
// }

sealed trait FeatureType
object FeatureType {
  case object Ordinal extends FeatureType
  case object Nominal extends FeatureType
  case object Continuous extends FeatureType
  case object Sparse extends FeatureType
}

case class FeatureMapping[A](
  mapping: Map[String, (FeatureType, A => FeatureValue)]
) {
  def featureType(key: String): FeatureType = mapping(key)._1
  def extract(key: String, value: A): FeatureValue = mapping(key)._2.apply(value)
}
