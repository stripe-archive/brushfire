package com.stripe.brushfire

import com.twitter.algebird._
import spire.algebra.PartialOrder

sealed trait Dispatched[+A, +B, +C, +D]

case class Ordinal[A](ordinal: A) extends Dispatched[A, Nothing, Nothing, Nothing]
case class Nominal[B](nominal: B) extends Dispatched[Nothing, B, Nothing, Nothing]
case class Continuous[C](continuous: C) extends Dispatched[Nothing, Nothing, C, Nothing]
case class Sparse[D](sparse: D) extends Dispatched[Nothing, Nothing, Nothing, D]

case class DispatchedSplitter[A, B, C, D, T](
  ordinal: Splitter[A, T],
  nominal: Splitter[B, T],
  continuous: Splitter[C, T],
  sparse: Splitter[D, T])
    extends Splitter[Dispatched[A, B, C, D], T] {

  type S = Dispatched[ordinal.S, nominal.S, continuous.S, sparse.S]

  val semigroup =
    Semigroup.from[S] {
      case (Ordinal(l), Ordinal(r)) => Ordinal(ordinal.semigroup.plus(l, r))
      case (Nominal(l), Nominal(r)) => Nominal(nominal.semigroup.plus(l, r))
      case (Continuous(l), Continuous(r)) => Continuous(continuous.semigroup.plus(l, r))
      case (Sparse(l), Sparse(r)) => Sparse(sparse.semigroup.plus(l, r))
      case (a, b) => sys.error("Values do not match: " + (a, b))
    }

  def create(value: Dispatched[A, B, C, D], target: T) = {
    value match {
      case Ordinal(a) => Ordinal(ordinal.create(a, target))
      case Nominal(b) => Nominal(nominal.create(b, target))
      case Continuous(c) => Continuous(continuous.create(c, target))
      case Sparse(d) => Sparse(sparse.create(d, target))
    }
  }

  def split(parent: T, stats: S) = stats match {
    case Ordinal(as) => Dispatched.wrapSplits(ordinal.split(parent, as))(Dispatched.ordinal)
    case Nominal(bs) => Dispatched.wrapSplits(nominal.split(parent, bs))(Dispatched.nominal)
    case Continuous(cs) => Dispatched.wrapSplits(continuous.split(parent, cs))(Dispatched.continuous)
    case Sparse(ds) => Dispatched.wrapSplits(sparse.split(parent, ds))(Dispatched.sparse)
  }
}

object Dispatched {
  implicit def dispatchedPartialOrder[A, B, C, D](implicit
    ordinalOrder: PartialOrder[A],
    nominalOrder: PartialOrder[B],
    continuousOrder: PartialOrder[C],
    sparseOrder: PartialOrder[D]
  ): PartialOrder[Dispatched[A, B, C, D]] =
    new PartialOrder[Dispatched[A, B, C, D]] {
      def partialCompare(left: Dispatched[A, B, C, D], right: Dispatched[A, B, C, D]): Double =
        (left, right) match {
          case (Ordinal(l), Ordinal(r)) => ordinalOrder.partialCompare(l, r)
          case (Nominal(l), Nominal(r)) => nominalOrder.partialCompare(l, r)
          case (Continuous(l), Continuous(r)) => continuousOrder.partialCompare(l, r)
          case (Sparse(l), Sparse(r)) => sparseOrder.partialCompare(l, r)
          case _ => Double.NaN
        }
    }

  def ordinal[A](a: A): Dispatched[A, Nothing, Nothing, Nothing] = Ordinal(a)
  def nominal[B](b: B): Dispatched[Nothing, B, Nothing, Nothing] = Nominal(b)
  def continuous[C](c: C): Dispatched[Nothing, Nothing, C, Nothing] = Continuous(c)
  def sparse[D](d: D): Dispatched[Nothing, Nothing, Nothing, D] = Sparse(d)

  def wrapSplits[X, T, A, B, C, D](splits: Iterable[Split[X, T]])(fn: X => Dispatched[A, B, C, D]) =
    splits.map(_.map(fn))
}
