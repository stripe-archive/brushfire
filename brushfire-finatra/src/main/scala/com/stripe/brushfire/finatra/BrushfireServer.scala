package com.stripe.brushfire.finatra

import com.stripe.brushfire._
import com.twitter.finatra._
import com.twitter.finagle.http._
import com.twitter.bijection._

class BrushfireServer extends FinatraServer {
  def score[K, V, T, P](root: String, trees: Iterable[Tree[K, V, T]], voter: Voter[T, P])(fn: ParamMap => Map[K, V]) = {
    register(new Controller {
      get(root) { request =>
        val row = fn(request.params)
        val score = voter.predict(trees, row)
        render.json(score).toFuture
      }
    })
  }

  def loadAndScore[K, V, T, P](root: String, treePath: String, voter: Voter[T, P])(fn: ParamMap => Map[K, V])(implicit inj: Injection[Tree[K, V, T], String]) = {
    val trees = scala.io.Source.fromFile(treePath).getLines.map { line =>
      val parts = line.split("\t")
      inj.invert(parts(1)).get
    }
    score[K, V, T, P](root, trees.toList, voter)(fn)
  }
}
