package com.stripe.brushfire.finatra

import com.stripe.brushfire._

object Main {
  import JsonInjections._

  def main(args: Array[String]) {
    implicit val weigher: (Map[String, Long] => Double) = _.values.sum.toDouble

    val treePath = args(0)
    val srv = new BrushfireServer
    srv.loadAndScore("/iris", treePath, SoftVoter[String, Long]) { _.mapValues { _.toDouble } }
    srv.main
  }
}
