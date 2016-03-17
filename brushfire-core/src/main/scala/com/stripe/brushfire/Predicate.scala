package com.stripe.brushfire

import spire.algebra.PartialOrder
import spire.implicits._

/**
 * A `Predicate` is a function which accepts or rejects feature values.
 *
 * Given a value of type `V`, `apply()` will return a Boolean
 * indicating whether the predicate matches or not.
 *
 * There are six types of predicates:
 *
 *  - IsEq(c): x == c
 *  - NotEq(c): x != c
 *  - Lt(c): x < c
 *  - LtEq(c): x <= c
 *  - Gt(c): x > c
 *  - GtEq(c): x >= c
 *
 * Predicates can be negated using `!`, and can be transformed using
 * `map`. Evaluating a predicate requires a PartialOrder[V], but this
 * constraint is not enforced during construction, only when `apply()`
 * is invoked.
 */
sealed abstract class Predicate[V] extends Product with Serializable {

  import Predicate._

  /**
   * Evaluate this predicate for the feature value `v`.
   */
  def apply(v: V)(implicit ord: PartialOrder[V]): Boolean =
    this match {
      case IsEq(x) => v === x
      case NotEq(x) => v =!= x
      case Lt(x) => v < x
      case LtEq(x) => v <= x
      case Gt(x) => v > x
      case GtEq(x) => v >= x
    }

  /**
   * Negate this predicate.
   *
   * The resulting predicate will return true in cases where this
   * predicate returns false.
   *
   * (Since all predicates return true for missing features, that
   * behavior is not negated by this method.)
   */
  def unary_!(): Predicate[V] =
    this match {
      case IsEq(v) => NotEq(v)
      case NotEq(v) => IsEq(v)
      case Lt(v) => GtEq(v)
      case LtEq(v) => Gt(v)
      case Gt(v) => LtEq(v)
      case GtEq(v) => Lt(v)
    }

  /**
   * Map the value types of this [[Predicate]] using `f`.
   *
   * Remember that in order to evaluate a Predicate[U] you will need
   * to be able to provide a valid PartialOrder[U] instance.
   */
  def map[U](f: V => U): Predicate[U] =
    this match {
      case IsEq(v) => IsEq(f(v))
      case NotEq(v) => NotEq(f(v))
      case Lt(v) => Lt(f(v))
      case LtEq(v) => LtEq(f(v))
      case Gt(v) => Gt(f(v))
      case GtEq(v) => GtEq(f(v))
    }

  /**
   * Display this predicate, using the given feature name as a
   * placeholder.
   */
  def display(name: String): String =
    this match {
      case IsEq(v) => s"$name == $v"
      case NotEq(v) => s"$name != $v"
      case Lt(v) => s"$name < $v"
      case LtEq(v) => s"$name <= $v"
      case Gt(v) => s"$name > $v"
      case GtEq(v) => s"$name >= $v"
    }

  /**
   * Display this predicate as a string.
   */
  override def toString(): String =
    display("x")
}


object Predicate {

  // specific predicate types
  case class IsEq[V](value: V) extends Predicate[V]
  case class NotEq[V](value: V) extends Predicate[V]
  case class Lt[V](value: V) extends Predicate[V]
  case class LtEq[V](value: V) extends Predicate[V]
  case class Gt[V](value: V) extends Predicate[V]
  case class GtEq[V](value: V) extends Predicate[V]

  // predicate factory constructors, to help fix the correct return
  // type (Predicate[V]).
  def isEq[V](x: V): Predicate[V] = IsEq(x)
  def notEq[V](x: V): Predicate[V] = NotEq(x)
  def lt[V](x: V): Predicate[V] = Lt(x)
  def ltEq[V](x: V): Predicate[V] = LtEq(x)
  def gt[V](x: V): Predicate[V] = Gt(x)
  def gtEq[V](x: V): Predicate[V] = GtEq(x)
}
