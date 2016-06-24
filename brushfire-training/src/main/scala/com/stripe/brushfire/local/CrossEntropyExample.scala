package com.stripe.brushfire.local

import com.stripe.bonsai._
import com.stripe.brushfire._
import com.twitter.algebird._

import AnnotatedTree.{AnnotatedTreeTraversal, fullBinaryTreeOpsForAnnotatedTree}

object CrossEntropyExample extends Defaults {

  def main(args: Array[String]) {
    val path = args.head
    val cols = args.tail.toList

    // T = Map[String, Long]
    // P = String
    // E = Double

    val trainingData = Lines(path).map { line =>
      val parts = line.split(",").reverse.toList
      val label = parts.head
      val values = parts.tail.map { s => s.toDouble }
      Instance(line, 0L, Map(cols.zip(values): _*), Map(label -> 1L))
    }.toIterable

    val totalData: Map[String, Long] = Monoid.sum(trainingData.iterator.map(_.target))
    val totalNormalized: Map[String, Double] = CrossEntropyError.normalize(totalData)
    val totalEntropy: Double = CrossEntropyError.entropy(totalNormalized)

    def relativeEntropy(xn: (Double, Long)): Double = {
      val (x, n) = xn
      (totalEntropy - (x / n)) / totalEntropy
    }

    val t0 = Trainer(trainingData, KFoldSampler(4)).updateTargets

    (0 until 10).foldLeft(t0) { (trainer, i) =>
      println(i)
      val error = trainer.validate(CrossEntropyError())
      println(error.map(relativeEntropy))
      trainer.expand(1)
    }
  }
}
