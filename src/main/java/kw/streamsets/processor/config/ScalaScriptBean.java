package kw.streamsets.processor.config;

import com.streamsets.pipeline.api.ConfigDef;


public class ScalaScriptBean {
    private static final String DEFAULT_SCRIPT =
            "/**\n" +
                    " * 处理程序执行脚本,\n" +
                    " * 脚本语言为scala语言，会在运行开始时候加载所有执行。\n" +
                    " * 数据处理的逻辑由传入RECORD或则BATCH的函数实现\n" +
                    " * 传入destroy的函数用于程序销毁\n" +
                    " * \n" +
                    " * log:Logger 用于日志输出\n" +
                    " * context:Context 当前processor的context,提供sdc相关功能\n" +
                    " * def err(record: Record, message: String): Unit 错误数据处理\n" +
                    " * def RECORD(f: (Record, Maker) => Unit): Unit\n" +
                    " * def BATCH(f: (Iterator[Record], Maker) => Unit): Unit\n" +
                    " * def destroy(f: => Unit): Unit **/\n" +
                    "RECORD{  (record,maker) =>\n" +
                    "    //在这里写你的处理逻辑\n" +
                    "    //@record : com.streamsets.pipeline.api.Record\n" +
                    "    //@maker  : com.streamsets.pipeline.api.base.SingleLaneProcessor.SingleLaneBatchMaker\n" +
                    "    maker.addRecord(record)\n" +
                    "}\n" +
                    "destroy{\n" +
                    "    log.info(\"程序结束\")\n" +
                    "}\n";
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            defaultValue = DEFAULT_SCRIPT,
            label = "scala脚本",
            description = "脚本将在启动是初始化，在处理时候调用RECORD或BATCH函数，程序销毁时效用after函数",
            displayPosition = 20,
            group = "#0",
            mode = ConfigDef.Mode.SCALA
    )
    public String initScript = "";
}
