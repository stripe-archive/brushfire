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
          val dims =
            toks.iterator.take(64).map(_.toDouble).toVector ++
            (1.to(64)).map{i=>scala.util.Random.nextInt(16).toDouble}
          val name = toks(64)
          bldr += ((dims, name))
          line = br.readLine()
        }
        bldr.result
      } finally {
        br.close()
      }
    }

    def accuracy(t: MondrianForest[Map[String,Long]], ps: TraversableOnce[(Vector[Double], String)]): Double = {
      ps.map{case (dims, label) =>
        val dist = t.predict(dims)
        val predicted = dist.toList.maxBy(_._2)._1
        if(predicted == label)
          1.0
        else
          0.0
      }.sum / ps.size
    }

    def l2(validation: Map[String,Long], training: Map[String,Long]):Double = {
      val size = training.values.sum
      val probs = training.mapValues{_.toDouble / size}
      validation.map{case (k,v) =>
        val err = 1.0 - probs.getOrElse(k,0.0)
        v * err * err
      }.sum
    }

  def acc(validation: Map[String,Long], training: Map[String,Long]):Double = {
    val predicted = training.toList.maxBy(_._2)._1
    validation.values.sum.toDouble - validation.getOrElse(predicted, 0L)
  }


    "do stuff with digits" in {

      val data = readDigitsData()

      //randomly partition into a training and validation set
      val (train, validate) = data.partition{d => math.random < 0.8}

      //train a forest. lambda is chosen to end up with around 3000 total nodes
      val t = MondrianForest(100, train.map{case (dims, name) => (dims, Map(name->1L))}, 0.005)

      //validate with the holdouts
      val validationAccuracy = accuracy(t, validate)

      //prune with l2 error, regularized by adding a constant for each leaf node
      var t2 = t

      println(t2.trees.map(_.size).sum)
      println(accuracy(t2, validate))

      (1.to(5)).foreach{i =>
        t2 = t2.pruneBy{case (v1,v2) => l2(v1,v2)+0.1}
        println(t2.trees.map(_.size).sum)
        println(accuracy(t2, validate))
        //retrain
        scala.util.Random.shuffle(train).foreach{
          case (xs,v) => t2 = t2.absorb(xs,Map(v->1L))
        }
        println(t2.trees.map(_.size).sum)
        println(accuracy(t2, validate))
      }

      //validate with the holdouts
      val validationAccuracy2 = accuracy(t2, validate)

      val tSize = t.trees.map(_.size).sum
      val t2Size = t2.trees.map(_.size).sum


      //despite having fewer total nodes, ...
      println(s"$t2Size < $tSize")
      t2Size should be < tSize

      //... pruning should yield greater accuracy
      println(s"$validationAccuracy2 > $validationAccuracy")
      validationAccuracy2 should be > validationAccuracy
    }
  }
}
