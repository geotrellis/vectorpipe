package vectorpipe.util

import java.io.InputStream

object Resource {
  def apply(name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(s"/$name")
    try {
      scala.io.Source.fromInputStream(stream).getLines.mkString(" ")
    } finally {
      stream.close()
    }
  }
}
