package com.stripe.brushfire

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._
import com.twitter.bijection.json._
import com.twitter.bijection.Inversion.{ attempt, attemptWhen }
import com.twitter.bijection.InversionFailure.{ failedAttempt, partialFailure }
import JsonNodeInjection._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.JsonMappingException
import org.codehaus.jackson.node.JsonNodeFactory
import scala.util._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object JsonInjections {

  def fromSingletonObject[A](n: JsonNode)(f: (String, JsonNode) => Try[A]): Try[A] =
    n.getFields.asScala.toList match {
      case entry :: Nil => f(entry.getKey, entry.getValue)
      case Nil => failedAttempt("no elements: " + n)
      case _ => failedAttempt("too many elements: " + n)
    }

  implicit def mapInjection[L, W](implicit labelInj: Injection[L, String], weightInj: JsonNodeInjection[W]): JsonNodeInjection[Map[L, W]] =
    new AbstractJsonNodeInjection[Map[L, W]] {

      def apply(frequencies: Map[L, W]): JsonNode =
        toJsonNode(frequencies.map { case (k, v) => (labelInj(k), toJsonNode(v)) })

      override def invert(n: JsonNode): Try[Map[L, W]] =
        Try {
          n.getFields.asScala.map { e =>
            (labelInj.invert(e.getKey).get, weightInj.invert(e.getValue).get)
          }.toMap
        }.recoverWith(partialFailure(n))
    }

  implicit def dispatchJsonNodeInjection[A: JsonNodeInjection, B: JsonNodeInjection, C: JsonNodeInjection, D: JsonNodeInjection]: JsonNodeInjection[Dispatched[A, B, C, D]] = new AbstractJsonNodeInjection[Dispatched[A, B, C, D]] {

    def apply(dispatched: Dispatched[A, B, C, D]): JsonNode = {
      val obj = JsonNodeFactory.instance.objectNode
      dispatched match {
        case Ordinal(v) => obj.put("ordinal", toJsonNode(v))
        case Nominal(v) => obj.put("nominal", toJsonNode(v))
        case Continuous(v) => obj.put("continuous", toJsonNode(v))
        case Sparse(v) => obj.put("sparse", toJsonNode(v))
      }
      obj
    }

    override def invert(n: JsonNode): Try[Dispatched[A, B, C, D]] =
      fromSingletonObject(n) {
        case ("ordinal", v) => fromJsonNode[A](v).map(Ordinal(_))
        case ("nominal", v) => fromJsonNode[B](v).map(Nominal(_))
        case ("continuous", v) => fromJsonNode[C](v).map(Continuous(_))
        case ("sparse", v) => fromJsonNode[D](v).map(Sparse(_))
        case _ => failedAttempt("Not a dispatched node: " + n)
      }
  }

  implicit def optionJsonNodeInjection[T: JsonNodeInjection] = new AbstractJsonNodeInjection[Option[T]] {
    def apply(opt: Option[T]): JsonNode = {
      val ary = JsonNodeFactory.instance.arrayNode
      opt.foreach { t => ary.add(toJsonNode(t)) }
      ary
    }

    def invert(n: JsonNode): Try[Option[T]] =
      n.getElements.asScala.toList match {
        case c :: Nil => fromJsonNode[T](c).map(Some(_))
        case Nil => Success(Option.empty[T])
        case _ => failedAttempt("Not an option: " + n)
      }
  }

  implicit val bool2String: Injection[Boolean, String] = new AbstractInjection[Boolean, String] {
    def apply(b: Boolean): String =
      b.toString
    override def invert(s: String): Try[Boolean] =
      s match {
        case "true" => Success(true)
        case "false" => Success(false)
        case _ => failedAttempt(s)
      }
  }

  implicit def predicateJsonInjection[V](implicit vInj: JsonNodeInjection[V]): JsonNodeInjection[Predicate[V]] = {
    new AbstractJsonNodeInjection[Predicate[V]] {
      import Predicate._

      def apply(pred: Predicate[V]): JsonNode = {
        val obj = JsonNodeFactory.instance.objectNode
        pred match {
          case IsEq(v) => obj.put("isEq", toJsonNode(v))
          case NotEq(v) => obj.put("notEq", toJsonNode(v))
          case Lt(v) => obj.put("lt", toJsonNode(v))
          case LtEq(v) => obj.put("ltEq", toJsonNode(v))
          case Gt(v) => obj.put("gt", toJsonNode(v))
          case GtEq(v) => obj.put("gtEq", toJsonNode(v))
        }
        obj
      }

      override def invert(n: JsonNode): Try[Predicate[V]] =
        fromSingletonObject(n) { (k, v) =>
          val t: Try[V] = fromJsonNode[V](v)
          k match {
            case "isEq" => t.map(IsEq(_))
            case "notEq" => t.map(NotEq(_))
            case "lt" => t.map(Lt(_))
            case "ltEq" => t.map(LtEq(_))
            case "gt" => t.map(Gt(_))
            case "gtEq" => t.map(GtEq(_))
            case k => failedAttempt(s"unknown predicate type: $k")
          }
        }
    }
  }

  implicit def treeJsonInjection[K, V, T, A](
    implicit kInj: JsonNodeInjection[K],
    pInj: JsonNodeInjection[T],
    vInj: JsonNodeInjection[V],
    // A common case is that A is unit, so we special case it here and avoid
    // serializing a whole bunch of units (and finding a sane serialization for
    // it). We do this by using `WithFallback`. If A is a unit, then we don't
    // need an injection. OTOH, if A isn't Unit, then we get an injection and
    // use that to serialize the annotation.
    maybeAInj: (Unit =:= A) WithFallback JsonNodeInjection[A]
  ): JsonNodeInjection[AnnotatedTree[K, V, T, A]] = {

    implicit def nodeJsonNodeInjection: JsonNodeInjection[Node[K, V, T, A]] =
      new AbstractJsonNodeInjection[Node[K, V, T, A]] {

        def apply(node: Node[K, V, T, A]): JsonNode =
          node match {
            case LeafNode(index, target, annotation) =>
              val obj = JsonNodeFactory.instance.objectNode
              obj.put("leaf", toJsonNode(index))
              obj.put("distribution", toJsonNode(target))
              // Don't serialize the annotation if we *know* it is Unit.
              maybeAInj.withFallback { aInj =>
                obj.put("annotation", aInj(annotation))
              }
              obj

            case SplitNode(k, p, lc, rc, annotation) =>
              val obj = JsonNodeFactory.instance.objectNode
              obj.put("key", toJsonNode(k))
              obj.put("predicate", toJsonNode(p)(predicateJsonInjection))
              obj.put("left", toJsonNode(lc)(nodeJsonNodeInjection))
              obj.put("right", toJsonNode(rc)(nodeJsonNodeInjection))
              // Don't serialize the annotation if we *know* it is Unit.
              maybeAInj.withFallback { aInj =>
                obj.put("annotation", aInj(annotation))
              }
              obj
          }

        def tryLoad[T: JsonNodeInjection](node: JsonNode, property: String): Try[T] =
          node.get(property) match {
            case null => Failure(new IllegalArgumentException(property + " != null"))
            case c => fromJsonNode[T](c)
          }

        def getAnnotation(node: JsonNode): Try[A] =
          maybeAInj match {
            case Preferred(unitToA) => Success(unitToA(()))
            case Fallback(aInj) => tryLoad[A](node, "annotation")(aInj)
          }

        override def invert(n: JsonNode): Try[Node[K, V, T, A]] =
          if (n.has("leaf")) {
            for {
              index <- tryLoad[Int](n, "leaf")
              target <- tryLoad[T](n, "distribution")
              annotation <- getAnnotation(n)
            } yield LeafNode(index, target, annotation)
          } else {
            for {
              k <- tryLoad[K](n, "key")
              p <- tryLoad[Predicate[V]](n, "predicate")
              left <- tryLoad[Node[K, V, T, A]](n, "left")(nodeJsonNodeInjection)
              right <- tryLoad[Node[K, V, T, A]](n, "right")(nodeJsonNodeInjection)
              annotation <- getAnnotation(n)
            } yield SplitNode(k, p, left, right, annotation)
          }
      }

    new AbstractJsonNodeInjection[AnnotatedTree[K, V, T, A]] {
      def apply(tree: AnnotatedTree[K, V, T, A]): JsonNode =
        toJsonNode(tree.root)
      override def invert(n: JsonNode): Try[AnnotatedTree[K, V, T, A]] =
        fromJsonNode[Node[K, V, T, A]](n).map(root => AnnotatedTree(root))
    }
  }

  implicit def treeJsonStringInjection[K, V, T, A](implicit jsonInj: JsonNodeInjection[AnnotatedTree[K, V, T, A]]): Injection[AnnotatedTree[K, V, T, A], String] =
    JsonInjection.toString[AnnotatedTree[K, V, T, A]]
}

object KryoInjections {
  implicit def tree2Bytes[K, V, T]: Injection[Tree[K, V, T], Array[Byte]] = new AbstractInjection[Tree[K, V, T], Array[Byte]] {
    override def apply(a: Tree[K, V, T]): Array[Byte] =
      KryoInjection(a)
    // we cast here because KryoInjection only works with Any. :/
    override def invert(b: Array[Byte]): Try[Tree[K, V, T]] =
      KryoInjection.invert(b).asInstanceOf[Try[Tree[K, V, T]]]
  }

  implicit def tree2String[K, V, T]: Injection[Tree[K, V, T], String] =
    Injection.connect[Tree[K, V, T], Array[Byte], Base64String, String]
}
