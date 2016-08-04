package com.stripe.brushfire.mondrian

import com.twitter.algebird.{ AdaptiveVector, HashingTrickMonoid, Monoid }
import scala.util.{Failure, Success, Try}
import scala.language.implicitConversions

class DataReader(bits: Int) {
  val monoid = new HashingTrickMonoid[Double](bits)

  implicit def toBytes(s: String): Array[Byte] = s.getBytes

  def tsvLine(str: String, ignore: Set[Int]): AdaptiveVector[Double] =
    Monoid.sum(str.split('\t')
      .iterator
      .zipWithIndex
      .filterNot { case (_, idx) => ignore(idx) }
      .map { case (s, idx) => parse(idx, s) })

  def parse(i: Int, value: String): AdaptiveVector[Double] =
    Try(value.toDouble) match {
      case Success(d) =>
        monoid.init((i.toString, d))
      case Failure(_) =>
        monoid.init((i.toString + value), 1.0)
    }
}
