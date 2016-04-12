package com.stripe.brushfire.local

import com.stripe.bonsai._
import com.stripe.brushfire._
import com.twitter.algebird._
import spire.algebra.{ Order, PartialOrder }

import AnnotatedTree.{AnnotatedTreeTraversal, fullBinaryTreeOpsForAnnotatedTree}

object Example extends Defaults {

  def main(args: Array[String]) {
    val path = args.head
    val cols = args.tail.toList

    val trainingData = Lines(path).map { line =>
      val parts = line.split(",").reverse.toList
      val label = parts.head
      val values = parts.tail.map { s => s.toDouble }
      Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
    }.toIterable

    var trainer =
      Trainer(trainingData, KFoldSampler(4))
        .updateTargets

    println(trainer.validate(AccuracyError()))

    1.to(10).foreach { i =>
      trainer = trainer.expand(1)
      println(trainer.validate(AccuracyError()))
    }
  
    //implicit val ord = Order.by[AveragedValue, Double] { _.value }
  }
}
