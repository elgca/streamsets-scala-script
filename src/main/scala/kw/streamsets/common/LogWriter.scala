package kw.streamsets.common

import java.io.Writer

import org.slf4j.Logger


class LogWriter(log: Logger) extends Writer {
  def close() = flush()

  def flush() = Console.flush()

  def write(cbuf: Array[Char], off: Int, len: Int) {
    if (len > 0)
      write(new String(cbuf.slice(off, off + len)))
  }

  override def write(str: String) {
    System.out.println(str)
    Option(str.trim).filterNot(_.isEmpty).foreach(log.info)
  }
}

