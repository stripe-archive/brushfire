package com.stripe.brushfire.finatra

import com.twitter.finatra._

class Example extends Controller {
  get("/") { request =>
    render.plain("hi").toFuture
  }
}

class ExampleServer extends FinatraServer {
  val controller = new Example()
  register(controller)
}

object Main {
  def main(args: Array[String]) {
    val srv = new ExampleServer
    srv.main
  }
}
