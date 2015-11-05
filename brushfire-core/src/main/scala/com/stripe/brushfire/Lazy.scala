package com.stripe.brushfire

import com.twitter.algebird.{Monoid, Semigroup}

/**
  * Wrapper class to allow expensive [[Semigroup]] operations on a [[Tree]] to be deferred until first use
  */
class Lazy[A](fn: () => A) extends Serializable {
  lazy val get: A = fn()
}

object Lazy {
  def apply[A](value: => A): Lazy[A] = {
    new Lazy[A](() => value)
  }

  implicit def semigroup[A: Semigroup]: Semigroup[Lazy[A]] = {
    new LazySemigroup[A]()
  }

  implicit def monoid[A: Monoid]: Monoid[Lazy[A]] = {
    new LazyMonoid[A]()
  }
}

class LazySemigroup[A: Semigroup] extends Semigroup[Lazy[A]] {
  override def plus(l: Lazy[A], r: Lazy[A]): Lazy[A] = {
    new Lazy(() => Semigroup.plus(l.get, r.get))
  }
}

class LazyMonoid[A: Monoid] extends LazySemigroup[A] with Monoid[Lazy[A]] {
  override lazy val zero: Lazy[A] = {
    new Lazy(() => Monoid.zero[A])
  }
}
