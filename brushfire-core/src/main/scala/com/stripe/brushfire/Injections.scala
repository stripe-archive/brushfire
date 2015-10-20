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

  implicit object UnitInjection extends JsonNodeInjection[Unit] {
    override def apply(a: Unit): JsonNode = {
      JsonNodeFactory.instance.objectNode
    }

    override def invert(b: JsonNode): Try[Unit] = {
      Success(())
    }
  }

  private def tryChild(node: JsonNode, property: String): Try[JsonNode] = Try {
    val child = node.get(property)
    assert(child != null, property + " != null")
    child
  }

  private def tryFromJsonNode[T: JsonNodeInjection](node: JsonNode, property: String): Try[T] = {
    tryChild(node, property)
      .flatMap(fromJsonNode[T])
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

  implicit def treeJsonInjection[K, V, T, A: Semigroup](
    implicit kInj: JsonNodeInjection[K],
    pInj: JsonNodeInjection[T],
    vInj: JsonNodeInjection[V],
    aInj: JsonNodeInjection[A],
    mon: Monoid[T],
    ord: Ordering[V] = null): JsonNodeInjection[Tree[K, V, T, A]] = {

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
            case _ => sys.error("Not a predicate node")
          }
        }
      }

    implicit def nodeJsonNodeInjection: JsonNodeInjection[Node[K, V, T, A]] =
      new AbstractJsonNodeInjection[Node[K, V, T, A]] {
        def apply(node: Node[K, V, T, A]) = node match {
          case LeafNode(index, target, _) =>
            val obj = JsonNodeFactory.instance.objectNode
            obj.put("leaf", toJsonNode(index))
            obj.put("distribution", toJsonNode(target))
            obj

          case SplitNode(annotation, children) =>
            val ary = JsonNodeFactory.instance.arrayNode
            children.foreach {
              case (feature, predicate, child) =>
                val obj = JsonNodeFactory.instance.objectNode
                obj.put("feature", toJsonNode(feature))
                obj.put("predicate", toJsonNode(predicate))
                obj.put("display", toJsonNode(Predicate.display(predicate)))
                obj.put("children", toJsonNode(child)(nodeJsonNodeInjection))
                ary.add(obj)
            }

            val obj = JsonNodeFactory.instance.objectNode
            obj.put("annotation", toJsonNode(annotation))
            obj.put("splits", ary)
            obj
        }

        private def invertLeaf(n: JsonNode, indexNode: JsonNode): Try[LeafNode[K, V, T, A]] = {
          for {
            index <- fromJsonNode[Int](indexNode)
            target <- fromJsonNode[T](n.get("distribution"))
            annotation <- fromJsonNode[A](n.get("annotation"))
          } yield {
            LeafNode(index, target, annotation)
          }
        }

        private def invertSplit(n: JsonNode): Try[SplitNode[K, V, T, A]] = {
          import Monad.scalaTry // needed for Applicative.sequence
          for {
            annotation <- fromJsonNode[A](n.get("annotation"))
            splitsNode <- tryChild(n, "splits")
            splits <- Applicative.sequence {
              splitsNode.getElements.asScala.toSeq.map { c =>
                for {
                  feature <- tryFromJsonNode[K](c, "feature")
                  predicate <- tryFromJsonNode[Predicate[V]](c, "predicate")
                  child <- tryFromJsonNode[Node[K, V, T, A]](c, "children")
                } yield {
                  (feature, predicate, child)
                }
              }
            }
          } yield {
            SplitNode[K, V, T, A](annotation, splits)
          }
        }

        override def invert(n: JsonNode) = {
          Option(n.get("leaf")) match {
            case Some(indexNode) => invertLeaf(n, indexNode)
            case None =>
              invertSplit(n).recoverWith {
                case e => Failure(InversionFailure(n, e))
              }
          }
        }
      }

    new AbstractJsonNodeInjection[Tree[K, V, T, A]] {
      def apply(tree: Tree[K, V, T, A]) = toJsonNode(tree.root)
      override def invert(n: JsonNode) = fromJsonNode[Node[K, V, T, A]](n).map { root => Tree(root) }
    }
  }

  implicit def treeJsonStringInjection[K, V, T, A](implicit jsonInj: JsonNodeInjection[Tree[K, V, T, A]]): Injection[Tree[K, V, T, A], String] =
    JsonInjection.toString[Tree[K, V, T, A]]
}

object KryoInjections {
  implicit def tree2Bytes[K, V, T, A]: Injection[Tree[K, V, T, A], Array[Byte]] = new AbstractInjection[Tree[K, V, T, A], Array[Byte]] {
    override def apply(a: Tree[K, V, T, A]) = KryoInjection(a)
    override def invert(b: Array[Byte]) = KryoInjection.invert(b).asInstanceOf[util.Try[Tree[K, V, T, A]]]
  }

  implicit def tree2String[K, V, T, A]: Injection[Tree[K, V, T, A], String] = Injection.connect[Tree[K, V, T, A], Array[Byte], Base64String, String]
}
