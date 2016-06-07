package com.stripe.brushfire.mondrian

package com.stripe.brushfire

import org.scalatest._
import org.scalatest.prop._

import org.scalacheck._
import Arbitrary.arbitrary

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

    def readIrisData(): Vector[(Vector[Double], String)] = {
      import java.io._
      val br = new BufferedReader(new FileReader(new File("example/iris.data")))
      try {
        var line = br.readLine()
        val bldr = Vector.newBuilder[(Vector[Double], String)]
        while (line != null) {
          val toks = line.split(',')
          val dims = toks.iterator.take(4).map(_.toDouble).toVector
          val name = toks(4)
          bldr += ((dims, name))
          line = br.readLine()
        }
        bldr.result
      } finally {
        br.close()
      }
    }

    "do stuff with flowers" in {

      val data = readIrisData()

      // ok let's train a tree on some flower data!
      val λ = 1000.0
      val t = MondrianTree(data.map(_._1), λ)

      // now let's evaluate our tree on the same data we used to train
      // it, grouped by its label (flower species).
      val labelled = data.groupBy(_._2).map { case (k, vs) =>
        (k, vs.map(_._1))
      }

      // we build histograms of the results and evaluate the overlap
      // across the different species of flowers.
      val histograms = labelled.map { case (k, xss) => (k, t.histogram(xss)) }
      val dotted = for {
        (s0, h0) <- histograms
        (s1, h1) <- histograms
      } yield {
        ((s0, s1), Util.dot(h0, h1))
      }

      // we have totally overfit this model (using a large λ
      // parameter), and are validating it with the training data, so
      // we are very confident that these classes will be distinct.
      dotted.foreach { case ((k0, k1), n) =>
        if (k0 != k1) {
          n shouldBe 0
        } else {
          n should be > 0
        }
      }
    }
  }
}
