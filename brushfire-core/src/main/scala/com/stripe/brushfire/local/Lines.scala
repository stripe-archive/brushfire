package com.stripe.brushfire.local

import java.io._
import scala.collection.generic.CanBuildFrom

abstract class Lines[A](val file: File, val charset: String = "UTF-8") { self =>

  def iterator: Iterator[A]

  def toIterable: Iterable[A] =
    new IterableLines(this)

  def filter(f: A => Boolean): Lines[A] =
    new Lines[A](file) {
      def iterator: Iterator[A] = self.iterator.filter(f)
    }

  def map[B](f: A => B): Lines[B] =
    new Lines[B](file) {
      def iterator: Iterator[B] = self.iterator.map(f)
    }

  def flatMap[B](f: A => Iterable[B]): Lines[B] =
    new Lines[B](file) {
      def iterator: Iterator[B] = self.iterator.flatMap(a => f(a).iterator)
    }

  def foldLeft[B](b0: B)(f: (B, A) => B): B =
    iterator.foldLeft(b0)(f)

  def foreach(f: A => Unit): Unit =
    iterator.foreach(f)

  override def toString(): String =
    s"Lines(<over $file with $charset>)"
}

object Lines {
  def apply(f: File, cs: String = "UTF-8"): Lines[String] =
    new Lines[String](f, cs) {
      def iterator: Iterator[String] =
        new Iterator[String] {
          println("Opening " + f)
          val reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset))
          var line: String = reader.readLine()
          if (line == null) reader.close()
          def hasNext(): Boolean = line != null
          def next(): String = {
            if (line == null) throw new NoSuchElementException("next on empty iterator")
            val out = line
            line = reader.readLine()
            if (line == null) reader.close()
            out
          }
        }
    }

  def apply(pathname: String): Lines[String] = apply(new File(pathname))
}

class IterableLines[A](lines: Lines[A]) extends Iterable[A] {
  def iterator: Iterator[A] = lines.iterator
}
