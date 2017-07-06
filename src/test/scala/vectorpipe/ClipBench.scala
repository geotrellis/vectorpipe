package vectorpipe

import geotrellis.vector._
import scaliper._

// --- //

class ClipBench extends Benchmarks with ConsoleReport {
/*
  benchmark("toNearestPoint - Java Style") {
    run("ALL IN - two-point line") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((1,1), (2,2))
        }

        def run() = Clip.toNearestPoint(extent, line)
      }
    }

    run("ALL IN - short line") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(-1000, -1000, 1000, 1000)
          line = Line((-10 to 10).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPoint(extent, line)
      }
    }

    run("ALL IN - medium line") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(-1000, -1000, 1000, 1000)
          line = Line((-100 to 100).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPoint(extent, line)
      }
    }

    run("MOST OUT - short line") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-10 to 10).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPoint(extent, line)
      }
    }

    run("MOST OUT - medium line") {
      new Benchmark {
        var extent: Extent = _
        var line: Line = _

        override def setUp() = {
          extent = Extent(0, 0, 5, 5)
          line = Line((-100 to 100).map(n => Point(n,n)))
        }

        def run() = Clip.toNearestPoint(extent, line)
      }
    }
  }
 */
}
