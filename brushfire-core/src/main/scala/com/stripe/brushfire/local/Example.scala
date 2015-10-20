package com.stripe.brushfire.local

import com.stripe.brushfire._
import com.twitter.algebird._

object Example extends Defaults {

  def main(args: Array[String]) {
    val cols = args.toList

    val trainingData = io.Source.stdin.getLines.map { line =>
      val parts = line.split(",").reverse.toList
      val label = parts.head
      val values = parts.tail.map { s => s.toDouble }
      Instance(DefaultMetadata(line, 0L), Map(cols.zip(values): _*), Map(label -> 1L))
    }.toList

    var trainer =
      Trainer(trainingData, KFoldSampler[DefaultMetadata](_.id, 4))
        .updateTargets

    println(trainer.validate(AccuracyError()))
    println(trainer.validate(BrierScoreError()))

    1.to(10).foreach { i =>
      trainer = trainer.expand(1)
      println(trainer.validate(AccuracyError()))
      println(trainer.validate(BrierScoreError()))
    }

    implicit val ord = Ordering.by[AveragedValue, Double] { _.value }
    trainer = trainer.prune(BrierScoreError())
    println(trainer.validate(AccuracyError()))
    println(trainer.validate(BrierScoreError()))
  }
}
