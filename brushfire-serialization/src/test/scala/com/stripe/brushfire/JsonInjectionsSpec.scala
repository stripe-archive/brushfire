package com.stripe.brushfire

import scala.util.Try

import com.twitter.bijection.json.JsonNodeInjection
import org.codehaus.jackson.node.JsonNodeFactory
import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers
import org.codehaus.jackson.JsonNode
import org.scalacheck.{ Gen, Arbitrary }
import org.scalacheck.Arbitrary.arbitrary

class JsonInjectionsSpec extends WordSpec with Matchers with Checkers {
  import TreeGenerators._
  import JsonInjections._
  import JsonNodeInjection._

  "treeJsonInjection" should {
    "round-trip without annotations" in {
      check { (tree: Tree[String, Double, Map[String, Long]]) =>
        val json = toJsonNode(tree)
        val haggard = fromJsonNode[Tree[String, Double, Map[String, Long]]](json)
        Try(tree) == haggard
      }
    }

    "round-trip with annotations" in {
      check { (tree: AnnotatedTree[String, Double, Map[String, Long], Int]) =>
        val json = toJsonNode(tree)
        val haggard = fromJsonNode[AnnotatedTree[String, Double, Map[String, Long], Int]](json)
        Try(tree) == haggard
      }
    }

    val jnf = JsonNodeFactory.instance

    val nullGen: Gen[JsonNode] = Gen.const(jnf.nullNode)
    val booleanGen: Gen[JsonNode] = arbitrary[Boolean].map(jnf.booleanNode)
    val numberGen: Gen[JsonNode] = arbitrary[Double].map(jnf.numberNode)
    val textGen: Gen[JsonNode] = arbitrary[String].map(jnf.textNode)

    def arrayGen(depth: Int): Gen[JsonNode] =
      Gen.listOf(jsonGen(depth)).map { ns =>
        val ary = jnf.arrayNode()
        ns.take(4).foreach(ary.add)
        ary
      }

    def objectGen(depth: Int): Gen[JsonNode] =
      Gen.mapOf(Gen.zip(arbitrary[String], jsonGen(depth))).map { m =>
        val obj = jnf.objectNode()
        m.take(4).foreach { case (k, v) => obj.put(k, v) }
        obj
      }

    def jsonGen(d: Int): Gen[JsonNode] =
      if (d <= 0) {
        Gen.frequency(
          (1, nullGen),
          (3, booleanGen),
          (6, numberGen),
          (8, textGen))
      } else {
        Gen.frequency(
          (1, nullGen),
          (3, booleanGen),
          (6, numberGen),
          (8, textGen),
          (2, arrayGen(d - 1)),
          (2, objectGen(d - 1)))
      }

    implicit val arbitraryJsonNode: Arbitrary[JsonNode] =
      Arbitrary(jsonGen(3))
    
    "never throw exceptions" in {
      check { (n: JsonNode) =>
        val t = fromJsonNode[AnnotatedTree[String, Double, Map[String, Long], Int]](n)
        true
      }
    }
  }
}
