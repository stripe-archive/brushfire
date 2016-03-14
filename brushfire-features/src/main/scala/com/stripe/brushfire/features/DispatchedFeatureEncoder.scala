package com.stripe.brushfire
package features

import scala.util.Try

import com.twitter.algebird.{ Aggregator, Semigroup, Monoid }

case class DispatchedFeatureEncoding[L](encoder: DispatchedFeatureEncoder)
extends FeatureEncoding[String, FeatureValue, Map[L, Long]] with Defaults {
  val splitter: Splitter[FeatureValue, Map[L, Long]] =
    DispatchedSplitter(
      BinarySplitter[Double, Map[L, Long]](LessThan(_)),
      BinarySplitter[String, Map[L, Long]](EqualTo(_)),
      doubleSplitter[Map[L, Long]],
      SpaceSaverSplitter[String, L]()
    )
}

object DispatchedFeatureEncoding {
  import DispatchedFeatureEncoder.Counts

  def trainer[L](
    platform: TrainingPlatform
  )(implicit
    ev: platform.AggregateContext[Map[String, Option[Counts]]]
  ): platform.Trainer[Map[String, FValue], DispatchedFeatureEncoding[L]] = {
    platform.Trainer.aggregate(DispatchedFeatureEncoder.aggregator).map(DispatchedFeatureEncoding[L](_))
  }
}

sealed trait DispatchedFeatureType
object DispatchedFeatureType {
  case object Ordinal extends DispatchedFeatureType
  case object Nominal extends DispatchedFeatureType
  case object Continuous extends DispatchedFeatureType
  case object Sparse extends DispatchedFeatureType
  case class BooleanCategory(tval: String, fval: String) extends DispatchedFeatureType
}

case class DispatchedFeatureEncoder(
  mappings: Map[String, DispatchedFeatureType]
) extends FeatureEncoder[String, FeatureValue] {
  def encode(value: Map[String, FValue]): Try[Map[String, FeatureValue]] = Try {
    val bldr = Map.newBuilder[String, FeatureValue]

    value.foreach { case (k, v) =>
      mappings.get(k).foreach { tpe =>
        val dispatched = tpe match {
          case DispatchedFeatureType.Ordinal =>
            Ordinal(v.tryAsDouble.get)
          case DispatchedFeatureType.Nominal =>
            Nominal(v.tryAsString.get)
          case DispatchedFeatureType.Continuous =>
            Continuous(v.tryAsDouble.get)
          case DispatchedFeatureType.Sparse =>
            Sparse(v.tryAsString.get)
          case DispatchedFeatureType.BooleanCategory(t, f) =>
            if (v.tryAsBoolean(t, f).get) {
              Nominal(t)
            } else {
              Nominal(f)
            }
        }

        bldr += (k -> dispatched)
      }
    }

    bldr.result()
  }
}

object DispatchedFeatureEncoder {
  val aggregator: Aggregator[Map[String, FValue], Map[String, Option[Counts]], DispatchedFeatureEncoder] =
    MapAggregator[String, FValue, Option[Counts], Option[DispatchedFeatureType]](Counts.aggregator)
      .andThenPresent { ts =>
        DispatchedFeatureEncoder(ts.collect { case (key, Some(value)) => key -> value })
      }

  def isDouble(s: String): Boolean = Try(s.toDouble).isSuccess

  case class UniqueCount[A](uniques: Option[Set[A]]) {
    def ++ (that: UniqueCount[A]): UniqueCount[A] =
      UniqueCount(for {
        a <- this.uniques
        b <- that.uniques
        c = a ++ b
        if (c.size < 20)
      } yield c)
  }

  object UniqueCount {
    def apply[A](a: A): UniqueCount[A] = UniqueCount(Some(Set(a)))
  }

  sealed trait Counts {
    import DispatchedFeatureType._

    def featureType: DispatchedFeatureType = this match {
      case DynamicBooleanCounts(t, f, c) =>
        BooleanCategory(t, f)

      case DynamicNumberCounts(u, c) =>
        if (u.uniques.isDefined) Ordinal else Continuous

      case TextCounts(u, c) =>
        if (u.uniques.isDefined) Nominal else Sparse

      case BooleanCounts(c) =>
        BooleanCategory("true", "false")

      case NumberCounts(u, c) =>
        if (u.uniques.isDefined) Ordinal else Continuous
    }

    def ++(that: Counts): Option[Counts] = (this, that) match {
      case (DynamicBooleanCounts(t1, f1, c1), DynamicBooleanCounts(t2, f2, c2)) if t1 == t2 =>
        Some(DynamicBooleanCounts(t1, f1, c1 + c2))

      case (DynamicBooleanCounts(t1, f1, c1), DynamicBooleanCounts(t2, f2, c2)) =>
        Some(TextCounts(UniqueCount(Some(Set(t1, f1, t2, f2))), c1 + c2))

      case (DynamicBooleanCounts(t1, f1, c1), DynamicNumberCounts(u2, c2)) =>
        Some(TextCounts(u2 ++ UniqueCount(Some(Set(t1, f1))), c1 + c2))

      case (DynamicNumberCounts(u1, c1), DynamicBooleanCounts(t2, f2, c2)) =>
        Some(TextCounts(u1 ++ UniqueCount(Some(Set(t2, f2))), c1 + c2))

      case (DynamicBooleanCounts(t1, f1, c1), TextCounts(u2, c2)) =>
        Some(TextCounts(UniqueCount(Some(Set(t1, f1))) ++ u2, c1 + c2))

      case (TextCounts(u1, c1), DynamicBooleanCounts(t2, f2, c2)) =>
        Some(TextCounts(u1 ++ UniqueCount(Some(Set(t2, f2))), c1 + c2))

      case (DynamicNumberCounts(u1, c1), DynamicNumberCounts(u2, c2)) =>
        Some(DynamicNumberCounts(u1 ++ u2, c1 + c2))

      case (DynamicNumberCounts(u1, c1), TextCounts(u2, c2)) =>
        Some(TextCounts(u1 ++ u2, c1 + c2))

      case (TextCounts(u1, c1), DynamicNumberCounts(u2, c2)) =>
        Some(TextCounts(u1 ++ u2, c1 + c2))

      case (TextCounts(u1, c1), TextCounts(u2, c2)) =>
        Some(TextCounts(u1 ++ u2, c1 + c2))

      case (BooleanCounts(c1), BooleanCounts(c2)) =>
        Some(BooleanCounts(c1 + c2))

      case (NumberCounts(u1, c1), NumberCounts(u2, c2)) =>
        Some(NumberCounts(u1 ++ u2, c1 + c2))

      case _ =>
        None // Incompatible, mismatched types.
    }
  }

  object Counts {
    def apply(value: FValue): Option[Counts] = value match {
      case FValue.FText(value) =>
        Some(TextCounts(UniqueCount(value), 1))
      case FValue.FNumber(value) =>
        Some(NumberCounts(UniqueCount(value), 1))
      case FValue.FBoolean(value) =>
        Some(BooleanCounts(1))
      case FValue.FDynamic(value) =>
        DynamicNumberCounts(value) orElse
        DynamicBooleanCounts(value) orElse
        Some(TextCounts(UniqueCount(value), 1))
      case FValue.FNull =>
        None
    }

    val CountsSemigroup: Semigroup[Option[Counts]] =
      Semigroup.from[Option[Counts]] {
        case (Some(a), Some(b)) => a ++ b
        case _ => None
      }

    val aggregator: Aggregator[FValue, Option[Counts], Option[DispatchedFeatureType]] =
      new Aggregator[FValue, Option[Counts], Option[DispatchedFeatureType]] {
        def prepare(input: FValue): Option[Counts] = Counts(input)
        def semigroup: Semigroup[Option[Counts]] = CountsSemigroup
        def present(reduction: Option[Counts]): Option[DispatchedFeatureType] =
          reduction.map(_.featureType)
      }
  }

  case class DynamicBooleanCounts(tval: String, fval: String, count: Long) extends Counts
  object DynamicBooleanCounts {
    def apply(value: String): Option[DynamicBooleanCounts] = value match {
      case "TRUE" | "FALSE" => Some(DynamicBooleanCounts("TRUE", "FALSE", 1))
      case "true" | "false" => Some(DynamicBooleanCounts("true", "false", 1))
      case "T" | "F" => Some(DynamicBooleanCounts("T", "F", 1))
      case "t" | "f" => Some(DynamicBooleanCounts("t", "f", 1))
      case "YES" | "NO" => Some(DynamicBooleanCounts("YES", "NO", 1))
      case "yes" | "no" => Some(DynamicBooleanCounts("yes", "no", 1))
      case "Y" | "N" => Some(DynamicBooleanCounts("Y", "N", 1))
      case "y" | "n" => Some(DynamicBooleanCounts("y", "n", 1))
      case _ => None
    }
  }

  case class DynamicNumberCounts(uniques: UniqueCount[String], count: Long) extends Counts
  object DynamicNumberCounts {
    def apply(value: String): Option[DynamicNumberCounts] =
      if (isDouble(value)) {
        Some(DynamicNumberCounts(UniqueCount[String](value), 1L))
      } else {
        None
      }
  }

  case class TextCounts(uniques: UniqueCount[String], count: Long) extends Counts
  object TextCounts {
    def apply(value: String): TextCounts = TextCounts(UniqueCount(value), 1)
  }

  case class BooleanCounts(count: Long) extends Counts

  case class NumberCounts(uniques: UniqueCount[Double], count: Long) extends Counts

  case class MapAggregator[K, -A, B, +C](agg: Aggregator[A, B, C])
  extends Aggregator[Map[K, A], Map[K, B], Map[K, C]] {
    def prepare(input: Map[K, A]): Map[K, B] =
      input.map { case (k, a) => k -> agg.prepare(a) }
    val semigroup: Semigroup[Map[K, B]] =
      Semigroup.mapSemigroup(agg.semigroup)
    def present(reduction: Map[K, B]): Map[K, C] =
      reduction.map { case (k, b) => k -> agg.present(b) }
  }
}
