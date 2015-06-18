package com.stripe.brushfire

/**
 * A `Predicate` is a function given a possibly missing feature value returns
 * `true` or `false`. It is generally used within a [[Tree]] to decide which
 * branch to follow while trying to classify a row/feature vector.
 */
sealed trait Predicate[V] extends (Option[V] => Boolean) {

  /**
   * Map the value types of this [[Predicate]] using `f`.
   */
  def map[V1: Ordering](f: V => V1): Predicate[V1] = this match {
    case EqualTo(v) => EqualTo(f(v))
    case LessThan(v) => LessThan(f(v))
    case Not(p) => Not(p.map(f))
    case AnyOf(list) => AnyOf(list.map(_.map(f)))
    case IsPresent(p) => IsPresent(p.map(_.map(f)))
  }
}

/**
 * A [[Predicate]] that returns `true` iff the input is missing or the input is
 * defined and is equal to `value` (according to the input's `equals` method).
 */
case class EqualTo[V](value: V) extends Predicate[V] {
  def apply(v: Option[V]) = v.isEmpty || (v.get == value)
}

/**
 * A [[Predicate]] that returns `true` iff the input is missing or if the input
 * is defined and it is *less than* `value`. This uses the implicit `Ordering`
 * of type `V` to handle the comparison.
 */
case class LessThan[V](value: V)(implicit ord: Ordering[V]) extends Predicate[V] {
  def apply(v: Option[V]) = v.isEmpty || ord.lt(v.get, value)
}

/**
 * A [[Predicate]] that returns `true` if `pred` returns `false` and returns
 * `false` if `pred` returns `true`.
 */
case class Not[V](pred: Predicate[V]) extends Predicate[V] {
  def apply(v: Option[V]) = v.isEmpty || !pred(v)
}

/**
 * A [[Predicate]] that returns `true` if any of the predicates in `preds`
 * returns `true`.
 */
case class AnyOf[V](preds: Seq[Predicate[V]]) extends Predicate[V] {
  def apply(v: Option[V]) = preds.exists { p => p(v) }
}

/**
 * A [[Predicate]] that will only return `true` if, at least, the value is
 * defined (not missing). Normally, predicates will treat a missing value (an
 * input of `None`) as a success and return `true`, but this is not always
 * desired. `IsPresent` allows an additional check that *requires* the value
 * be present to succeed.
 *
 * If `pred` is `None`, then this is a predicate that returns `true` iff the
 * value is present (not missing).
 */
case class IsPresent[V](pred: Option[Predicate[V]]) extends Predicate[V] {
  def apply(v: Option[V]) = v.isDefined && pred.fold(true)(_(v))
}

object Predicate {
  def display[V](predicate: Predicate[V]): String = {
    predicate match {
      case EqualTo(v) => "= " + v.toString
      case LessThan(v) => "< " + v.toString
      case Not(EqualTo(v)) => "!= " + v.toString
      case Not(LessThan(v)) => ">= " + v.toString
      case AnyOf(List(LessThan(v), EqualTo(u))) => "<= " + v.toString
      case AnyOf(List(EqualTo(v), LessThan(u))) => "<= " + v.toString
      case Not(AnyOf(List(LessThan(v), EqualTo(u)))) => "> " + v.toString
      case Not(AnyOf(List(EqualTo(v), LessThan(u)))) => "> " + v.toString
      case Not(p) => "!(" + display(p) + ")"
      case AnyOf(l) => l.map { p => "(" + display(p) + ")" }.mkString(" || ")
      case IsPresent(None) => "exists"
      case IsPresent(Some(pred)) => "(exists && (" + display(pred) + "))"
    }
  }
}
