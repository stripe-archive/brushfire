package com.stripe.brushfire

sealed trait Predicate[V] {
  def apply(v: Option[V]): Boolean
}

case class IsPresent[V]() extends Predicate[V] {
  def apply(v: Option[V]) = v.isDefined
}

case class EqualTo[V](value: V) extends Predicate[V] {
  def apply(v: Option[V]) = !v.isDefined || (v.get == value)
}

case class LessThan[V](value: V)(implicit ord: Ordering[V]) extends Predicate[V] {
  def apply(v: Option[V]) = !v.isDefined || ord.lt(v.get, value)
}

case class Not[V](pred: Predicate[V]) extends Predicate[V] {
  def apply(v: Option[V]) = !v.isDefined || !pred(v)
}

case class AnyOf[V](preds: Seq[Predicate[V]]) extends Predicate[V] {
  def apply(v: Option[V]) = preds.exists { p => p(v) }
}

object Predicate {
  def display[V](predicate: Predicate[V]): String = {
    predicate match {
      case IsPresent() => "exists"
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
    }
  }
}
