package com.stripe.brushfire.local

import com.stripe.brushfire._
import com.stripe.brushfire.training._
import com.twitter.algebird._
import com.twitter.bijection._

object Example extends Defaults {

  import JsonInjections._

  def main(args: Array[String]) {
    val cols = args.toList

    val trainingData = io.Source.stdin.getLines.map { line =>
      val parts = line.split(",").reverse.toList
      val label = parts.head
      val values = parts.tail.map { s => s.toDouble }
      Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
    }.toList

    var trainer =
      Trainer(trainingData, KFoldSampler(4))
        .updateTargets

    println(trainer.validate(AccuracyError()))
    println(trainer.validate(BrierScoreError()))

    1.to(10).foreach { i =>
      trainer = trainer.expand
      printTrees(trainer.trees)
      println(trainer.validate(AccuracyError()))
      println(trainer.validate(BrierScoreError()))
    }

    def printTrees[K,V,T](trees: List[Tree[K,V,T]])(implicit inj: Injection[Tree[K, V, T], String]) {
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
