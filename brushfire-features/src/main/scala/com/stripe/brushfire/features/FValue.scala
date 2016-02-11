package com.stripe.brushfire
package features

import scala.util.{ Try, Success, Failure }

sealed abstract class FValue {
  import FValue._

  def tryAsDouble: Try[Double] = this match {
    case FDynamic(value) => Try(value.toDouble)
    case FNumber(value) => Success(value)
    case FBoolean(_) => Failure(new NumberFormatException("cannot coerce boolean to double"))
    case FText(_) => Failure(new NumberFormatException("cannot coerce text to double"))
    case FNull => Failure(new NumberFormatException("cannot coerce null to double"))
  }

  def tryAsBoolean: Try[Boolean] = tryAsBoolean("true", "false")

  def tryAsBoolean(tval: String, fval: String): Try[Boolean] = this match {
    case FBoolean(value) => Success(value)
    case FDynamic(`tval`) => Success(true)
    case FDynamic(`fval`) => Success(false)
    case FDynamic(value) => Failure(new IllegalArgumentException(s"cannot convert dynamic '$value' to boolean"))
    case FNumber(_) => Failure(new IllegalArgumentException("cannot convert number to boolean"))
    case FText(_) => Failure(new IllegalArgumentException("cannot convert text to boolean"))
    case FNull => Failure(new IllegalArgumentException("cannot convert null to boolean"))
  }

  def tryAsString: Try[String] = this match {
    case FDynamic(value) => Success(value)
    case FText(value) => Success(value)
    case FNumber(_) => Failure(new IllegalArgumentException("cannot convert number to text"))
    case FBoolean(_) => Failure(new IllegalArgumentException("cannot convert boolean to text"))
    case FNull => Failure(new IllegalArgumentException("cannot convert null to text"))
  }
}

object FValue {
  def dynamic(value: String): FValue = FDynamic(value)
  def boolean(value: Boolean): FValue = FBoolean(value)
  def text(value: String): FValue = FText(value)
  def number(value: Double): FValue = FNumber(value)

  case class FDynamic(value: String) extends FValue
  case class FBoolean(value: Boolean) extends FValue
  case class FText(value: String) extends FValue
  case class FNumber(value: Double) extends FValue
  case object FNull extends FValue
}
