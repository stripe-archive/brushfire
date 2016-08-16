package com.stripe.brushfire
package mondrian

import com.stripe.brushfire.local._

import AnnotatedTree.{AnnotatedTreeTraversal, fullBinaryTreeOpsForAnnotatedTree}

import org.scalatest._
import org.scalatest.prop._

import org.scalacheck._
import Arbitrary.arbitrary

import com.twitter.algebird._

import scala.util.Random

case class Lambda(toDouble: Double)

object Lambda {
  implicit val arbitraryLambda: Arbitrary[Lambda] =
    Arbitrary(Gen.chooseNum(1.0, 1000000.0).map(Lambda(_)))
}

case class Vect3(toVector: Vector[Double])

object Vect3 {
  implicit val arbitraryVect3: Arbitrary[Vect3] =
    Arbitrary(arbitrary[(Double, Double, Double)]
      .map { case (x, y, z) => Vect3(Vector(x, y, z)) })
}

class PredicateSpec extends WordSpec with Matchers with PropertyChecks with Defaults {

  "MondrianTree" should {

    "be created from points" in {
      forAll { (xss: Iterable[Vect3], lambda: Lambda) =>
        val t = MondrianTree(xss.map(_.toVector), lambda.toDouble)
        t.root.isEmpty shouldBe xss.isEmpty
        t.size should be <= xss.size
      }
    }

    def readDigitsData(): Vector[(Vector[Double], String)] = {
      import java.io._
      val br = new BufferedReader(new FileReader(new File("example/digits.data")))
      try {
        var line = br.readLine()
        val bldr = Vector.newBuilder[(Vector[Double], String)]
        while (line != null) {
          val toks = line.split(',')
          val dims = toks.iterator.take(64).map(_.toDouble).toVector
          val name = toks(64)
          bldr += ((dims, name))
          line = br.readLine()
        }
        bldr.result
      } finally {
        br.close()
      }
    }

/**
 * cross entropy is -\sum_i p_i log q_i
 */
    def crossEntropy(truth: Boolean, prob: Double): Double = {
      val eps = 0.0001
      def clamp(p: Double): Double =
        if (p <= 0.0) eps
        else if (p >= 1.0) (1.0 - eps)
        else p

      -(if (truth) math.log(clamp(prob)) else math.log1p(-clamp(prob)))/math.log(2.0)
    }

    def relativeCE(data: List[(Boolean, Double)]): (Double, Double, Double) = {
      val truth = data.map { case (t, _) => if (t) 1.0 else 0.0 }
      val p = (truth.sum) / (truth.size)
      val entropy =
        if (p == 0.0 || p == 1.0) 0.0
        else -(p * math.log(p) + (1.0 - p) * math.log1p(-p))/math.log(2.0)

      val ce = data.map { case (t, p) => crossEntropy(t, p) }.sum / truth.size
      ((entropy - ce)/entropy, entropy, ce)
    }

    def getCrossEntropy(t: MondrianForest[Map[String,Long]], ps: TraversableOnce[(Vector[Double], String)]): (Double, Double, Double) =
      relativeCE(ps.map {case (dims, label) =>
        val dist = t.predict(dims)
        val trueP = dist.getOrElse("True", 0L)
        val falseP = dist.getOrElse("False", 0L)
        val p = trueP.toDouble/(trueP + falseP)
        val truth = label == "True"
        (truth, p)
      }.toList)

    def accuracy(t: MondrianForest[Map[String,Long]], ps: TraversableOnce[(Vector[Double], String)]): Double =
      ps.map{case (dims, label) =>
        val dist = t.predict(dims)
        val predicted = dist.toList.maxBy(_._2)._1
        if(predicted == label)
          1.0
        else
          0.0
      }.sum / ps.size

    def l2(map: Map[String,Long]):Double = {
      val size = map.values.sum
      val probs = map.mapValues{_.toDouble / size}
      map.map{case (k,v) =>
        val err = 1.0 - probs(k)
        v * err * err
      }.sum
    }

    "do stuff with digits" in {

      val data: Vector[(Vector[Double], String)] = readDigitsData()

      val trainingData: Iterable[Instance[String, Double, Map[String, Long]]] =
        data.iterator.zipWithIndex.map { case ((dims, label), n) =>
          val feats = dims.iterator.zipWithIndex.map { case (x, i) => (i.toString, x) }.toMap
          Instance(n.toString, n.toLong, feats, Map(label -> 1L))
        }.toVector

      def time() = System.currentTimeMillis
      val ms0 = time()

      //val rft0 = Trainer(trainingData, KFoldSampler(4)).updateTargets
      //val rounds = 10
      val rft0 = Trainer(trainingData, RFSampler(10, 0.3, 1.0)).updateTargets
      val rounds = 30
      val rft = (0 until rounds).foldLeft(rft0) { (trainer, i) => trainer.expand(1000) }
      def lsize(n: Node[_, _, _, _]): Int =
        n.fold((lc, rc, _) => lsize(lc) + lsize(rc), _ => 1)
      val rfn = rft.trees.map(t => lsize(t.root)).sum
      val cee = rft.validate(CrossEntropyError()).get.value

      val ms1 = time()
      val rf_ms = ms1 - ms0

      //randomly partition into a training and validation set
      val (train, validate) = data.partition{d => math.random < 0.8}

      //train a forest. lambda is chosen to end up with around 3000 total nodes
      //val t = MondrianForest(100, train.map{case (dims, name) => (dims, Map(name->1L))}, 0.006)
      val numTrees = 10
      val lambda = 0.025
      //val lambda = Double.PositiveInfinity
      val t = MondrianForest(numTrees, train.map{case (dims, name) => (dims, Map(name->1L))}, lambda)

      val ms2 = time()
      val mf1_ms = ms2 - ms1

      //validate with the holdouts
      val validationAccuracy = accuracy(t, validate)

      //train another forest, pruning every 100 examples
      //lambda is higher to compensate for pruning
      //this usually ends up around 2500 total nodes
      var t2 = MondrianForest.empty[Map[String,Long]](numTrees, Double.PositiveInfinity)
      val groups = train.grouped(1000).foreach { batch =>
        //prune with l2 error, regularized by adding a constant for each leaf node
        t2 = t2.pruneBy{v => l2(v) + 2.0}
        batch.foreach{case (xs,v) => t2 = t2.absorb(xs,Map(v->1L))}
      }

      val mf2_ms = time() - ms2

      val xe2 = Monoid.sum(validate.iterator.map { case (dims, lbl) =>
        val p = Map(lbl -> 1.0)
        val q = t2.predict(dims).mapValues(_.toDouble)
        CrossEntropyError.crossEntropyError(p, q)
      }).value

      //validate with the holdouts
      val validationAccuracy2 = accuracy(t2, validate)

      val xe = Monoid.sum(validate.iterator.map { case (dims, lbl) =>
        val p = Map(lbl -> 1.0)
        val q = t.predict(dims).mapValues(_.toDouble)
        CrossEntropyError.crossEntropyError(p, q)
      }).value

      val n = validate.size
      val freqs = validate
        .groupBy { case (_, lbl) => lbl }
        .mapValues(_.size.toDouble / n) // normalized/pseudo-clamped

      val en = StableSum(freqs.iterator.map { case (_, p) => -p * math.log(p) })

      println("")
      println(s"mf digits: lambda = $lambda")
      println(s"mf digits: entropy = $en")
      println(s"mf digits: cross-entropy = $xe")
      val rxe = (en - xe) / en
      println(s"mf train time: $mf1_ms")
      println(s"mf digits: relative cross-entropy = $rxe")
      println(s"mf #trees/#nodes: ${t.trees.size}/${t.trees.map(_.size).sum}")
      println("")
      val rxe2 = (en - xe2) / en
      println(s"mf2 train time: $mf2_ms")
      println(s"mf2 digits: relative cross-entropy = $rxe2")
      println(s"mf2 #trees/#nodes: ${t2.trees.size}/${t2.trees.map(_.size).sum}")
      println("")

      println(s"rf digits: entropy = $en")
      println(s"rf digits: cross-entropy = $cee")
      val rcee = (en - cee) / en
      println(s"rf train time: $rf_ms")
      println(s"rf digits: relative cross-entropy = $rcee")
      println(s"rf #trees/#nodes: ${rft.trees.size}/$rfn")
      println("")

      //despite having fewer total nodes, ...
      t2.trees.map(_.size).sum should be < t.trees.map(_.size).sum

      // //... pruning should yield greater accuracy
      // validationAccuracy2 should be > validationAccuracy

      //print the absolute numbers for human gratification
      println(s"mf digits: $validationAccuracy2 > $validationAccuracy")
    }

    // "try training with hashing" in {
    //   val path = "sample131k.tsv"
    //   val dataReader = new DataReader(9)
    //   //val ignore = Set(0, 1, 40, 41) // chargeid and date
    //   //val data = io.Source.fromFile("sample131k.tsv").getLines.map { line =>
    //   val ignore = Set(40) // last one is the label
    //   //val data = io.Source.fromFile("transformed.tsv").getLines.take(1<<17).map { line =>
    //   val data: List[(Vector[Double], String)] =
    //     io.Source.fromFile(path).getLines.map { line =>
    //       val v: Vector[Double] = dataReader.tsvLine(line, ignore).toVector
    //       val label = line.split('\t')(40)
    //       (v, label)
    //     }.toList
    //   //randomly partition into a training and validation set
    //   //val (train, validate) = data.partition{d => math.random < 0.8}
    //   //val prob = data.count { case (_, t) => t == "True" }.toDouble / data.size
    //   //println(s"prob: $prob, size: ${data.size}")
    // 
    //   val trues = data.filter { case (_, t) => t == "True" }
    //   val falses = Random.shuffle(data.filter { case (_, t) => t == "False" }).take(trues.size)
    //   val balanced = Random.shuffle(trues ++ falses)
    //   val (train, validate) = balanced.partition{d => math.random < 0.8}
    //   val prob = balanced.count { case (_, t) => t == "True" }.toDouble / balanced.size
    // 
    //   val trainingData: Iterable[Instance[Int, Double, Map[String, Long]]] =
    //     balanced.iterator.zipWithIndex.map { case ((dims, label), n) =>
    //       val feats = dims.iterator.zipWithIndex.map { case (x, i) => (i, x) }.toMap
    //       Instance(n.toString, n.toLong, feats, Map(label -> 1L))
    //     }.toVector
    // 
    //   println("training...")
    //   val trainer0 = Trainer(trainingData, KFoldSampler(4))
    //   println("updating...")
    //   val trainer1 = trainer0.updateTargets
    //   val acc = trainer1.validate(AccuracyError())
    //   println(s"rf acc: $acc")
    // 
    //   println(s"prob: $prob, size: ${balanced.size}")
    //   //train a forest. lambda is chosen to end up with around 3000 total nodes
    //   var t = MondrianForest.empty[Map[String,Long]](100, 0.006)
    // 
    //   val groups = train.grouped(10000).foreach { batch =>
    //     //prune with l2 error, regularized by adding a constant for each leaf node
    //     //t = t.pruneBy { v => l2(v) + 2.0 }
    //     batch.foreach { case (xs,v) => t = t.absorb(xs,Map(v->1L)) }
    //   }
    // 
    //   //validate with the holdouts
    //   val (relce, e, ce) = getCrossEntropy(t, validate)
    // 
    //   val avg = Monoid.sum(validate.iterator.map { case (dims, lbl) =>
    //     val p = Map(lbl -> 1.0)
    //     val q = t.predict(dims).mapValues(_.toDouble)
    //     CrossEntropyError.crossEntropyError(p, q)
    //   }).value
    //   println(s"2: cross entropy = $avg")
    // 
    //   println(s"relative cross entropy: $relce, entropy: $e, cross entropy: $ce")
    // }
  }
}
