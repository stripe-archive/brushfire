package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Dispatched[+A, +B, +C, +D]

case class Ordinal[A](ordinal: A) extends Dispatched[A, Nothing, Nothing, Nothing]
case class Nominal[B](nominal: B) extends Dispatched[Nothing, B, Nothing, Nothing]
case class Continuous[C](continuous: C) extends Dispatched[Nothing, Nothing, C, Nothing]
case class Sparse[D](sparse: D) extends Dispatched[Nothing, Nothing, Nothing, D]

object Dispatched {
  implicit def ordering[A, B, C, D](implicit ordinalOrdering: Ordering[A], continuousOrdering: Ordering[C]): Ordering[Dispatched[A, B, C, D]] = new Ordering[Dispatched[A, B, C, D]] {
    def compare(left: Dispatched[A, B, C, D], right: Dispatched[A, B, C, D]) = (left, right) match {
      case (Ordinal(l), Ordinal(r)) => ordinalOrdering.compare(l, r)
      case (Continuous(l), Continuous(r)) => continuousOrdering.compare(l, r)
      case _ => sys.error("Values cannot be compared: " + (left, right))
    }
  }

  def ordinal[A](a: A) = Ordinal(a)
  def nominal[B](b: B) = Nominal(b)
  def continuous[C](c: C) = Continuous(c)
  def sparse[D](d: D) = Sparse(d)
}
