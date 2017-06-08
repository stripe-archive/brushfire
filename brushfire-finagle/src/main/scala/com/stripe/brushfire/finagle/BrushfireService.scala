package com.stripe.brushfire.finagle

import com.fasterxml.jackson.databind.ObjectMapper
import com.stripe.brushfire._
import com.twitter.algebird.Semigroup
import com.twitter.bijection.Injection
import com.twitter.bijection.json.JsonNodeInjection
import com.twitter.finagle.Service
import com.twitter.finagle.http.{ParamMap, Request, Response, Status, Version}
import com.twitter.util.{Future, Try}

class BrushfireService[K, V: Ordering, T: Semigroup, P](trees: Iterable[Tree[K, V, T]], voter: Voter[T, P])(
  fn: ParamMap => Map[K, V]
)(implicit
  injP: JsonNodeInjection[P]
) extends Service[Request, Response] {
  private val mapper = new ObjectMapper()

  def apply(request: Request): Future[Response] = Future {
    val row = fn(request.params)
    val score = voter.predict[K, V, Unit](trees, row)

    val response = Response(Version.Http11, Status.Ok)

    response.setContentTypeJson()
    response.setContentString(mapper.writeValueAsString(injP(score)))
    response
  }
}

object BrushfireService {
  def apply[K, V: Ordering, T: Semigroup, P](treePath: String, voter: Voter[T, P])(fn: ParamMap => Map[K, V])(implicit
    injTree: Injection[Tree[K, V, T], String],
    injP: JsonNodeInjection[P]
  ): Service[Request, Response] = {
    val trees = scala.io.Source.fromFile(treePath).getLines.map { line =>
      val parts = line.split("\t")
      injTree.invert(parts(1)).get
    }

    new BrushfireService[K, V, T, P](trees.toList, voter)(fn)
  }
}
