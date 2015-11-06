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
    "round-trip" in {
      check { (tree: Tree[String, Double, Map[String, Long], Unit]) =>
        val haggard = fromJsonNode[Tree[String, Double, Map[String, Long], Unit]](toJsonNode(tree))
        Try(tree) == haggard
      }
    }
  }
}
