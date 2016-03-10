package com.stripe.brushfire

import scala.util.Try

import com.twitter.bijection.json.JsonNodeInjection

import org.scalatest.{ WordSpec, Matchers }
import org.scalatest.prop.Checkers

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
  }
}
