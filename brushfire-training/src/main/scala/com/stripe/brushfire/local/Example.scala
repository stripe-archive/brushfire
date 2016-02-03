package com.stripe.brushfire.local

import com.stripe.bonsai._
import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.algebird._
import com.twitter.bijection._

import AnnotatedTree.{AnnotatedTreeTraversal, fullBinaryTreeOpsForAnnotatedTree}

object Example extends Defaults {

  import JsonInjections._

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
    println(trainer.validate(BrierScoreError()))

    1.to(10).foreach { i =>
      trainer = trainer.expand
//      printTrees(trainer.trees)
      println(trainer.validate(AccuracyError()))
      println(trainer.validate(BrierScoreError()))
    }

    def printTrees[K,V,T](trees: List[com.stripe.brushfire.Tree[K,V,T]])(implicit inj: Injection[com.stripe.brushfire.Tree[K, V, T], String]) {
      trees.foreach{tree => println(inj(tree))}
    }
/*
    implicit val ord = Ordering.by[AveragedValue, Double] { _.value }
    trainer = trainer.prune(BrierScoreError())
    println(trainer.validate(AccuracyError()))
    println(trainer.validate(BrierScoreError()))
*/
  }
}
