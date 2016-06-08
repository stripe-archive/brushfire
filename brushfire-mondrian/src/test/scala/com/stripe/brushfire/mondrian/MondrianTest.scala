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
  }
}
