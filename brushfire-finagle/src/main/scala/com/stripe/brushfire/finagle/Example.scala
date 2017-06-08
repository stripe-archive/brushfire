package com.stripe.brushfire.finagle

import com.stripe.brushfire._
import com.twitter.finagle.Http
import com.twitter.finagle.http.Method
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.service.RoutingService
import java.net.InetSocketAddress
import com.twitter.util.{Await, Future}

object Main {
  import JsonInjections._

  def main(args: Array[String]): Unit = {
    val treePath = args(0)

    val service = BrushfireService(treePath, SoftVoter[String, Long]) { _.mapValues { _.toDouble } }

    val router = RoutingService.byMethodAndPathObject {
      case Method.Get -> Root / "iris" => service
    }

    val server = Http.server.serve(":8080", router)

    println(Await.ready(server))
  }
}
