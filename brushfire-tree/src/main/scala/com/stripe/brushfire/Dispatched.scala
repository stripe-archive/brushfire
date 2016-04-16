package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Dispatched[+A, +B, +C, +D]

case class Ordinal[A](ordinal: A) extends Dispatched[A, Nothing, Nothing, Nothing]
case class Nominal[B](nominal: B) extends Dispatched[Nothing, B, Nothing, Nothing]
case class Continuous[C](continuous: C) extends Dispatched[Nothing, Nothing, C, Nothing]
case class Sparse[D](sparse: D) extends Dispatched[Nothing, Nothing, Nothing, D]

object Dispatched {
  implicit def ordering[A, B, C, D](implicit
    ordinalOrdering: Ordering[A],
    nominalOrdering: Ordering[B],
    continuousOrdering: Ordering[C],
    sparseOrdering: Ordering[D]
  ): Ordering[Dispatched[A, B, C, D]] = new Ordering[Dispatched[A, B, C, D]] {
    def compare(left: Dispatched[A, B, C, D], right: Dispatched[A, B, C, D]) = (left, right) match {
      case (Ordinal(l), Ordinal(r)) => ordinalOrdering.compare(l, r)
      case (Nominal(l), Nominal(r)) => nominalOrdering.compare(l, r)
      case (Continuous(l), Continuous(r)) => continuousOrdering.compare(l, r)
      case (Sparse(l), Sparse(r)) => sparseOrdering.compare(l, r)
      case _ => sys.error("Values cannot be compared: " + (left, right))
    }
  }

  def ordinal[A](a: A) = Ordinal(a)
  def nominal[B](b: B) = Nominal(b)
  def continuous[C](c: C) = Continuous(c)
  def sparse[D](d: D) = Sparse(d)

  def wrapSplits[X, T, A: Ordering, B, C: Ordering, D](splits: Iterable[Split[X, T]])(fn: X => Dispatched[A, B, C, D]) = splits.map(_.map(fn))
}
