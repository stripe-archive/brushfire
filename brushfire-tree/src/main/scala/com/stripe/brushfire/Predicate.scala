package com.stripe.brushfire

import com.stripe.bonsai.Layout
import com.stripe.bonsai.layout.ProductLayout

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
 * `map`. Evaluating a predicate requires an Ordering, but this
 * constraint is not enforced during construction, only when `apply()`
 * is invoked.
 */
sealed abstract class Predicate[V] extends Product with Serializable {

  import Predicate._

  /**
   * Returns the right-hand side of the predicate's comparison.
   */
  def value: V

  /**
   * Evaluate this predicate for the feature value `v`.
   */
  def apply(v: V)(implicit ord: Ordering[V]): Boolean =
    this match {
      case IsEq(x) => ord.equiv(v, x)
      case NotEq(x) => !ord.equiv(v, x)
      case Lt(x) => ord.lt(v, x)
      case LtEq(x) => ord.lteq(v, x)
      case Gt(x) => ord.gt(v, x)
      case GtEq(x) => ord.gteq(v, x)
    }

  /**
   * Negate this predicate.
   *
   * The resulting predicate will return true in cases where this
   * predicate returns false.
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
   * to be able to provide a valid Ordering[U] instance.
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

  private def unpack[V](pred: Predicate[V]): (Byte, V) =
    pred match {
      case IsEq(value) => (0.toByte, value)
      case NotEq(value) => (1.toByte, value)
      case Lt(value) => (2.toByte, value)
      case LtEq(value) => (3.toByte, value)
      case Gt(value) => (4.toByte, value)
      case GtEq(value) => (5.toByte, value)
    }

  private def pack[V](i: Byte, value: V): Predicate[V] =
    (i: @annotation.switch) match {
      case 0 => IsEq(value)
      case 1 => NotEq(value)
      case 2 => Lt(value)
      case 3 => LtEq(value)
      case 4 => Gt(value)
      case 5 => GtEq(value)
    }

  implicit def predicateLayout[V: Layout]: Layout[Predicate[V]] =
    new ProductLayout(Layout[Byte], Layout[V], unpack, pack)
}
