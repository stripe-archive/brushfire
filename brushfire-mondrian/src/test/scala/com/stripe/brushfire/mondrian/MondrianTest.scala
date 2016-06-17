package com.stripe.brushfire.mondrian


import org.scalatest._
import org.scalatest.prop._

import org.scalacheck._
import Arbitrary.arbitrary

import com.twitter.algebird._

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

class PredicateSpec extends WordSpec with Matchers with PropertyChecks {

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

      val data = readDigitsData()

      //randomly partition into a training and validation set
      val (train, validate) = data.partition{d => math.random < 0.8}

      //train a forest. lambda is chosen to end up with around 3000 total nodes
      val t = MondrianForest(100, train.map{case (dims, name) => (dims, Map(name->1L))}, 0.006)

      //validate with the holdouts
      val validationAccuracy = accuracy(t, validate)

      //train another forest, pruning every 100 examples
      //lambda is higher to compensate for pruning
      //this usually ends up around 2500 total nodes
      var t2 = MondrianForest.empty[Map[String,Long]](100, 0.01)
      val groups = train.grouped(100).foreach{batch =>
        //prune with l2 error, regularized by adding a constant for each leaf node
        t2 = t2.pruneBy{v => l2(v)+2.0}
        batch.foreach{case (xs,v) => t2 = t2.absorb(xs,Map(v->1L))}
      }

      //validate with the holdouts
      val validationAccuracy2 = accuracy(t2, validate)

      //despite having fewer total nodes, ...
      t2.trees.map(_.size).sum should be < t.trees.map(_.size).sum

      //... pruning should yield greater accuracy
      validationAccuracy2 should be > validationAccuracy

      //print the absolute numbers for human gratification
      println(s"$validationAccuracy2 > $validationAccuracy")
    }

    "try training with hashing" in {
      val dataReader = new DataReader(9)
      //val ignore = Set(0, 1, 40, 41) // chargeid and date
      //val data = io.Source.fromFile("sample131k.tsv").getLines.map { line =>
      val ignore = Set(40) // last one is the label
      val data = io.Source.fromFile("transformed.tsv").getLines.take(1<<17).map { line =>
        val v = dataReader.tsvLine(line, ignore).toVector
        val label = line.split('\t')(40)
        (v, label)
      }.toList
      //randomly partition into a training and validation set
      //val (train, validate) = data.partition{d => math.random < 0.8}
 //     val prob = data.count { case (_, t) => t == "True" }.toDouble / data.size
   //   println(s"prob: $prob, size: ${data.size}")

      val trues = data.filter { case (_, t) => t == "True" }
      val falses = scala.util.Random.shuffle(data.filter { case (_, t) => t == "False" })
        .take(trues.size)
      val balanced = scala.util.Random.shuffle(trues ++ falses)
      val (train, validate) = balanced.partition{d => math.random < 0.8}
      val prob = balanced.count { case (_, t) => t == "True" }.toDouble / balanced.size

      println(s"prob: $prob, size: ${balanced.size}")
      //train a forest. lambda is chosen to end up with around 3000 total nodes
      var t = MondrianForest.empty[Map[String,Long]](100, 0.006)
      val groups = train.grouped(10000).foreach { batch =>
        //prune with l2 error, regularized by adding a constant for each leaf node
        //t = t.pruneBy { v => l2(v) + 2.0 }
        batch.foreach { case (xs,v) => t = t.absorb(xs,Map(v->1L)) }
      }

      //validate with the holdouts
      val (relce, e, ce) = getCrossEntropy(t, validate)

      println(s"relative cross entropy: $relce, entropy: $e, cross entropy: $ce")
    }
  }
}
