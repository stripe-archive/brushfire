package com.stripe.brushfire

import com.twitter.algebird._

sealed trait Dispatched[+A, +B, +C, +D]

case class Ordinal[A](ordinal: A) extends Dispatched[A, Nothing, Nothing, Nothing]
case class Nominal[B](nominal: B) extends Dispatched[Nothing, B, Nothing, Nothing]
case class Continuous[C](continuous: C) extends Dispatched[Nothing, Nothing, C, Nothing]
case class Sparse[D](sparse: D) extends Dispatched[Nothing, Nothing, Nothing, D]

class DispatchedSplitter[A: Ordering, B, C: Ordering, D, T](
  val ordinal: Splitter[A, T],
  val nominal: Splitter[B, T],
  val continuous: Splitter[C, T],
  val sparse: Splitter[D, T])
    extends Splitter[Dispatched[A, B, C, D], T] {

  type S = Dispatched[ordinal.S, nominal.S, continuous.S, sparse.S]
  val semigroup =
    Semigroup.from[S] { (a, b) =>
      (a, b) match {
        case (Ordinal(l), Ordinal(r)) => Ordinal(ordinal.semigroup.plus(l, r))
        case (Nominal(l), Nominal(r)) => Nominal(nominal.semigroup.plus(l, r))
        case (Continuous(l), Continuous(r)) => Continuous(continuous.semigroup.plus(l, r))
        case (Sparse(l), Sparse(r)) => Sparse(sparse.semigroup.plus(l, r))
        case _ => sys.error("Values do not match: " + (a, b))
      }
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

  def wrapSplits[X, T, A: Ordering, B, C: Ordering, D](splits: Iterable[Split[X, T]])(fn: X => Dispatched[A, B, C, D]) = {
    splits.map { split =>
      new Split[Dispatched[A, B, C, D], T] {
        def predicates = split.predicates.map {
          case (pred, p) => (wrapPredicate(pred)(fn), p)
        }
      }
    }
  }

  def wrapPredicate[X, A: Ordering, B, C: Ordering, D](predicate: Predicate[X])(fn: X => Dispatched[A, B, C, D]): Predicate[Dispatched[A, B, C, D]] =
    predicate match {
      case IsPresent() => IsPresent()
      case EqualTo(v) => EqualTo(fn(v))
      case LessThan(v) => LessThan(fn(v))
      case Not(p) => Not(wrapPredicate(p)(fn))
      case AnyOf(list) => AnyOf(list.map { p => wrapPredicate(p)(fn) })
    }
}
