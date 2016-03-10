package com.stripe.brushfire

/**
 * Allows for typeclasses to be requested, but for fallbacks to be used if the
 * preferred typeclass isn't available. For example, if you'd prefer a `Group`
 * because the algorithm will run faster, but can make do with a `Monoid` you
 * could use an implicit like `implicit ev: Group[A] WithFallback Monoid[A]`.
 */
sealed abstract class WithFallback[A, B] {
  def fold[C](f: A => C, g: B => C): C = this match {
    case Preferred(a) => f(a)
    case Fallback(b) => g(b)
  }

  def withFallback[C](f: B => C): Option[C] =
    fold(_ => None, b => Some(f(b)))
}

final case class Preferred[A, B](value: A) extends WithFallback[A, B]
final case class Fallback[A, B](value: B) extends WithFallback[A, B]

trait WithFallbackLow {
  implicit def fallback[A, B](implicit ev: B): WithFallback[A, B] =
    new Fallback[A, B](ev)
}

object WithFallback extends WithFallbackLow {
  implicit def preferred[A, B](implicit ev: A): WithFallback[A, B] =
    new Preferred[A, B](ev)
}
