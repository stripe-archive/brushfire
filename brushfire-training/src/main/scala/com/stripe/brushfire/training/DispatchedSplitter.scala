package com.stripe.brushfire.training

import com.stripe.brushfire._
import com.twitter.algebird._

case class DispatchedSplitter[A: Ordering, B, C: Ordering, D, T](
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
    case Ordinal(as) => DispatchedSplitter.wrapSplits(ordinal.split(parent, as))(Dispatched.ordinal)
    case Nominal(bs) => DispatchedSplitter.wrapSplits(nominal.split(parent, bs))(Dispatched.nominal)
    case Continuous(cs) => DispatchedSplitter.wrapSplits(continuous.split(parent, cs))(Dispatched.continuous)
    case Sparse(ds) => DispatchedSplitter.wrapSplits(sparse.split(parent, ds))(Dispatched.sparse)
  }
}

object DispatchedSplitter {
   def wrapSplits[X, T, A: Ordering, B, C: Ordering, D](splits: Iterable[Split[X, T]])(fn: X => Dispatched[A, B, C, D]) =
    splits.map { case Split(p, left, right) => Split(p.map(fn), left, right) }
}
