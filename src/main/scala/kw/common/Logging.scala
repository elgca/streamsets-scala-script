package kw.common

import org.slf4j.{Logger, LoggerFactory}
//import org.apache.log4j.Logger
trait Logging {
//  protected val log = Logger.getLogger(this.getClass)
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
}
