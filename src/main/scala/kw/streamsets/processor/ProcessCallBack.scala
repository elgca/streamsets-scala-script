package kw.streamsets.processor

import com.streamsets.pipeline.api.Record
import com.streamsets.pipeline.api.base.SingleLaneProcessor.{SingleLaneBatchMaker => Maker}

/**
  * 存储来自scala脚本的函数
  */
class ProcessCallBack {
  private var batchFlag: Option[Boolean] = None
  private var afterF: () => Unit = () => ()
  private var processRecord: (Record, Maker) => Unit = _
  private var processBatch: (Iterator[Record], Maker) => Unit = _

  def isBatch: Boolean = batchFlag.get

  def isValid: Boolean = batchFlag.isDefined

  def destroy(f: => Unit): Unit = {
    afterF = () => f
  }

  def RECORD(f: (Record, Maker) => Unit): Unit = {
    batchFlag = Option(false)
    processRecord = f
  }

  def BATCH(f: (Iterator[Record], Maker) => Unit): Unit = {
    batchFlag = Option(true)
    processBatch = f
  }

  private[processor] def destroy(): Unit = afterF()

  private[processor] def process(record: Record, maker: Maker): Unit = processRecord(record, maker)

  private[processor] def process(records: Iterator[Record], maker: Maker): Unit = processBatch(records, maker)
}
