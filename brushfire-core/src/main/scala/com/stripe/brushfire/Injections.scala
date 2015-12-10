package com.stripe.brushfire

import com.twitter.algebird._
import com.twitter.bijection._
import com.twitter.chill._
import com.twitter.bijection.json._
import com.twitter.bijection.Inversion.{ attempt, attemptWhen }
import com.twitter.bijection.InversionFailure.{ failedAttempt, partialFailure }
import JsonNodeInjection._
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.JsonNodeFactory
import scala.util._
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object JsonInjections {
  implicit def mapInjection[L, W](implicit labelInj: Injection[L, String], weightInj: JsonNodeInjection[W]): JsonNodeInjection[Map[L, W]] =
    new AbstractJsonNodeInjection[Map[L, W]] {
      def apply(frequencies: Map[L, W]) =
        toJsonNode(frequencies.map { case (k, v) => labelInj(k) -> toJsonNode(v) })

      override def invert(n: JsonNode) =
        fromJsonNode[Map[String, W]](n).flatMap { map =>
          attempt(map) { m =>
            m.map { case (str, v) => labelInj.invert(str).toOption.get -> v }
          }
        }
    }

  implicit def dispatchJsonNodeInjection[A: JsonNodeInjection, B: JsonNodeInjection, C: JsonNodeInjection, D: JsonNodeInjection]: JsonNodeInjection[Dispatched[A, B, C, D]] = new AbstractJsonNodeInjection[Dispatched[A, B, C, D]] {
    def apply(dispatched: Dispatched[A, B, C, D]) = {
      val obj = JsonNodeFactory.instance.objectNode
      dispatched match {
        case Ordinal(v) => obj.put("ordinal", toJsonNode(v))
        case Nominal(v) => obj.put("nominal", toJsonNode(v))
        case Continuous(v) => obj.put("continuous", toJsonNode(v))
        case Sparse(v) => obj.put("sparse", toJsonNode(v))
      }
      obj
    }

    override def invert(n: JsonNode) = n.getFieldNames.asScala.toList.headOption match {
      case Some("ordinal") => fromJsonNode[A](n.get("ordinal")).map { Ordinal(_) }
      case Some("nominal") => fromJsonNode[B](n.get("nominal")).map { Nominal(_) }
      case Some("continuous") => fromJsonNode[C](n.get("continuous")).map { Continuous(_) }
      case Some("sparse") => fromJsonNode[D](n.get("sparse")).map { Sparse(_) }
      case _ => sys.error("Not a dispatched node: " + n)
    }
  }

  implicit def optionJsonNodeInjection[T: JsonNodeInjection] = new AbstractJsonNodeInjection[Option[T]] {
    def apply(opt: Option[T]) = {
      val ary = JsonNodeFactory.instance.arrayNode
      opt.foreach { t => ary.add(toJsonNode(t)) }
      ary
    }

    def invert(n: JsonNode) =
      n.getElements.asScala.toList.headOption match {
        case Some(c) => fromJsonNode[T](c).map { t => Some(t) }
        case None => Success(None: Option[T])
      }
  }

  implicit val bool2String: Injection[Boolean, String] = new AbstractInjection[Boolean, String] {
    def apply(b: Boolean) = b.toString
    override def invert(s: String) = s match {
      case "true" => Success(true)
      case "false" => Success(false)
      case _ => failedAttempt(s)
    }
  }

  implicit def treeJsonInjection[K, V, T](
    implicit kInj: JsonNodeInjection[K],
    pInj: JsonNodeInjection[T],
    vInj: JsonNodeInjection[V],
    mon: Monoid[T],
    ord: Ordering[V] = null): JsonNodeInjection[Tree[K, V, T]] = {

    implicit def predicateJsonNodeInjection: JsonNodeInjection[Predicate[V]] =
      new AbstractJsonNodeInjection[Predicate[V]] {
        def apply(pred: Predicate[V]) = {
          val obj = JsonNodeFactory.instance.objectNode
          pred match {
            case IsPresent(None) => obj.put("exists", JsonNodeFactory.instance.nullNode)
            case IsPresent(Some(pred)) => obj.put("exists", toJsonNode(pred)(predicateJsonNodeInjection))
            case EqualTo(v) => obj.put("eq", toJsonNode(v))
            case LessThan(v) => obj.put("lt", toJsonNode(v))
            case Not(pred) => obj.put("not", toJsonNode(pred)(predicateJsonNodeInjection))
            case AnyOf(preds) =>
              val ary = JsonNodeFactory.instance.arrayNode
              preds.foreach { pred => ary.add(toJsonNode(pred)(predicateJsonNodeInjection)) }
              obj.put("or", ary)
          }
          obj
        }

        override def invert(n: JsonNode) = {
          n.getFieldNames.asScala.toList.headOption match {
            case Some("eq") => fromJsonNode[V](n.get("eq")).map { EqualTo(_) }
            case Some("lt") =>
              if (ord == null)
                sys.error("No Ordering[V] supplied but less than used")
              else
                fromJsonNode[V](n.get("lt")).map { LessThan(_) }
            case Some("not") => fromJsonNode[Predicate[V]](n.get("not")).map { Not(_) }
            case Some("or") => fromJsonNode[List[Predicate[V]]](n.get("or")).map { AnyOf(_) }
            case Some("exists") =>
              val predNode = n.get("exists")
              if (predNode.isNull) Success(IsPresent[V](None))
              else fromJsonNode[Predicate[V]](predNode).map(p => IsPresent(Some(p)))
            case _ => sys.error("Not a predicate node: " + n)
          }
        }
      }

    implicit def nodeJsonNodeInjection: JsonNodeInjection[Node[K, V, T, Unit]] =
      new AbstractJsonNodeInjection[Node[K, V, T, Unit]] {
        def apply(node: Node[K, V, T, Unit]) = node match {
          case LeafNode(index, target, _) =>
            val obj = JsonNodeFactory.instance.objectNode
            obj.put("leaf", toJsonNode(index))
            obj.put("distribution", toJsonNode(target))
            obj

          case SplitNode(p, k, lc, rc, _) =>
            val obj = JsonNodeFactory.instance.objectNode
            obj.put("predicate", toJsonNode(p)(predicateJsonNodeInjection))
            obj.put("key", toJsonNode(k))
            obj.put("left", toJsonNode(lc)(nodeJsonNodeInjection))
            obj.put("right", toJsonNode(rc)(nodeJsonNodeInjection))
            obj
        }

        def tryChild(node: JsonNode, property: String): Try[JsonNode] =
          Try {
            val child = node.get(property)
            assert(child != null, property + " != null")
            child
          }

        def tryLoad[T: JsonNodeInjection](node: JsonNode, property: String): Try[T] = {
          val child = node.get(property)
          if (child == null) Failure(new IllegalArgumentException(property + " != null"))
          else fromJsonNode[T](child)
        }

        override def invert(n: JsonNode): Try[Node[K, V, T, Unit]] =
          if (n.has("leaf")) {
            for {
              index <- tryLoad[Int](n, "leaf")
              target <- tryLoad[T](n, "distribution")
            } yield LeafNode(index, target)
          } else {
            for {
              p <- tryLoad[Predicate[V]](n, "predicate")
              k <- tryLoad[K](n, "key")
              left <- tryLoad[Node[K, V, T, Unit]](n, "left")(nodeJsonNodeInjection)
              right <- tryLoad[Node[K, V, T, Unit]](n, "right")(nodeJsonNodeInjection)
            } yield SplitNode(p, k, left, right)
          }
      }

    new AbstractJsonNodeInjection[Tree[K, V, T]] {
      def apply(tree: Tree[K, V, T]): JsonNode =
        toJsonNode(tree.root)
      override def invert(n: JsonNode): Try[Tree[K, V, T]] =
        fromJsonNode[Node[K, V, T, Unit]](n).map(root => Tree(root))
    }
  }

  implicit def treeJsonStringInjection[K, V, T](implicit jsonInj: JsonNodeInjection[Tree[K, V, T]]): Injection[Tree[K, V, T], String] =
    JsonInjection.toString[Tree[K, V, T]]
}

object KryoInjections {
  implicit def tree2Bytes[K, V, T]: Injection[Tree[K, V, T], Array[Byte]] = new AbstractInjection[Tree[K, V, T], Array[Byte]] {
    override def apply(a: Tree[K, V, T]) = KryoInjection(a)
    override def invert(b: Array[Byte]) = KryoInjection.invert(b).asInstanceOf[util.Try[Tree[K, V, T]]]
  }

  implicit def tree2String[K, V, T]: Injection[Tree[K, V, T], String] = Injection.connect[Tree[K, V, T], Array[Byte], Base64String, String]
}
