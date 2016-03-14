package com.stripe.brushfire
package features

import scala.language.higherKinds

import com.twitter.algebird.{ Monad, Aggregator }

trait TrainingPlatform {
  type AggregateContext[A]

  trait Executor[M[_], A] {
    def aggregate[B: AggregateContext, C](agg: Aggregator[A, B, C]): M[C]
  }

  sealed trait Trainer[-A, +B]

  object Trainer {

    def pure[A](value: A): Trainer[Any, A] = Return(value)

    def aggregate[A, B, C](agg: Aggregator[A, B, C])(implicit ctx: AggregateContext[B]): Trainer[A, C] =
      AndThen[A, C, C](TrainingStep.Aggregate(agg)(ctx), pure(_))

    case class AndThen[-A, B, +C](step: TrainingStep[A, B], next: B => Trainer[A, C]) extends Trainer[A, C]
    case class Return[+A](value: A) extends Trainer[Any, A]

    private def joinAndThen[A, B1, C1, B2, C2](lhs: AndThen[A, B1, C1], rhs: AndThen[A, B2, C2]): Trainer[A, (C1, C2)] =
      joinStep(lhs.step, rhs.step) match {
        case Some(step) =>
          AndThen[A, (B1, B2), (C1, C2)](step, { case (b1, b2) => lhs.next(b1) join rhs.next(b2) })
        case None =>
          AndThen[A, B1, (C1, C2)](lhs.step, { b1 => lhs.next(b1) join rhs })
      }

    // We put most methods in this implicit class so that we can easily deal
    // with the variance of A and B.
    implicit class TrainerOps[A, B](self: Trainer[A, B]) {
      def contramap[A1](f: A1 => A): Trainer[A1, B] = self match {
        case AndThen(step, next) =>
          AndThen(composeStep(step, f), next.andThen(_.contramap(f)))
        case Return(value) =>
          Return(value)
      }

      def map[C](f: B => C): Trainer[A, C] =
        self.flatMap(b => Return(f(b)))

      def flatMap[C](f: B => Trainer[A, C]): Trainer[A, C] = self match {
        case (andThen: AndThen[_, b, _]) => // (step, next) =>
          AndThen(andThen.step, { (x: b) =>
            val trainer: Trainer[A, B] = andThen.next(x)
            trainer.flatMap(f) 
          })
        case Return(value) =>
          f(value)
      }

      def join[A2 <: A, B2](that: Trainer[A2, B2]): Trainer[A2, (B, B2)] = (self, that) match {
        case (lhs @ AndThen(step1, next1), rhs @ AndThen(step2, next2)) =>
          joinAndThen(lhs, rhs)
        case (AndThen(step, next), Return(b2)) =>
          self.map { b => (b, b2) }
        case (Return(b), AndThen(step, next)) =>
          that.map { b2 => (b, b2) }
        case (Return(b1), Return(b2)) =>
          Return((b1, b2))
      }

      def run[M[_]: Monad](executor: Executor[M, A]): M[B] = self match {
        case AndThen(step @ TrainingStep.Aggregate(agg), next) =>
          Monad.flatMap(executor.aggregate(agg)(step.context)) { s =>
            next(s).run(executor)
          }

        case Return(value) =>
          Monad[M].apply(value)
      }
    }
  }

  sealed trait TrainingStep[-A, +B]
  object TrainingStep {
    case class Aggregate[-A, B, +C](
      aggregator: Aggregator[A, B, C]
    )(implicit
      val context: AggregateContext[B]
    ) extends TrainingStep[A, C]
  }

  // Override this if some steps can be combined together efficiently.
  def joinStep[A, B1, B2](
    step1: TrainingStep[A, B1],
    step2: TrainingStep[A, B2]
  ): Option[TrainingStep[A, (B1, B2)]] = None

  private def composeStep[A1, A2, B](step: TrainingStep[A2, B], f: A1 => A2): TrainingStep[A1, B] = step match {
    case step @ TrainingStep.Aggregate(agg) => TrainingStep.Aggregate(agg.composePrepare(f))(step.context)
  }
}

final class DummyImplicit
object DummyImplicit {
  implicit val dummyImplicit: DummyImplicit = new DummyImplicit
}

trait ContextlessTrainingPlatform extends TrainingPlatform {
  type AggregateContext[A] = DummyImplicit

  override def joinStep[A, B1, B2](
    step1: TrainingStep[A, B1],
    step2: TrainingStep[A, B2]
  ): Option[TrainingStep[A, (B1, B2)]] = (step1, step2) match {
    case (TrainingStep.Aggregate(agg1), TrainingStep.Aggregate(agg2)) =>
      Some(TrainingStep.Aggregate(agg1 join agg2))
  }
}

object LocalTrainingPlatform extends ContextlessTrainingPlatform {
  case class IterableExecutor[A](values: Iterable[A]) extends Executor[({ type f[x] = x })#f, A] {
    def aggregate[B: AggregateContext, C](agg: Aggregator[A, B, C]): C =
      agg(values)
  }
}
