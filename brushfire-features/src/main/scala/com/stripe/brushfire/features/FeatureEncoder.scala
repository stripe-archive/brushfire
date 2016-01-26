package com.stripe.brushfire
package features

// sealed trait FeatureEncoder[I] {
// }

// Map[String, String]
// T <: ThriftStruct

sealed trait FeatureValue
object FeatureValue {
  case class FString(value: String) extends FeatureValue
  case class FDouble(value: Double) extends FeatureValue
  sealed abstract class FBoolean(val value: Boolean) extends FeatureValue
  case object FTrue extends FBoolean(true)
  case object FFalse extends FBoolean(false)
  case object FNull extends FeatureValue
}

sealed trait FeatureType
object FeatureType {
}

  

f: Map[String, (FeatureType, A => FeatureValue)]

./bin/train --features features.tsv --output model.json

trait FeatureTrainer[K, V, T] {
  // type S
  // Aggregator[(A, T), S, (FeatureEncoder[
  // case class Instance[K, V, T](id: String, timestamp: Long, features: Map[K, V], target: T)
}
