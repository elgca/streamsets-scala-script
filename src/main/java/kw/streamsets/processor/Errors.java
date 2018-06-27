package kw.streamsets.processor;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {
    SCRIPTING_00("获取scala引擎失败"),
    SCRIPTING_01("初始化scala失败"),
    SCRIPTING_02("脚本不为空"),
    SCRIPTING_03("脚本编译错误"),
    SCRIPTING_04("不存在回调函数"),
    SCRIPTING_05("record模式运行出错: {}"),
    SCRIPTING_06("batch 模式运行出错: {}"),
    SCRIPTING_07("来自脚本的错误数据: {}"),
    SCRIPTING_08("Script error while running init script: {}"),
    SCRIPTING_09("函数after运行错误: {}"),;
    private final String msg;

    Errors(String msg) {
        this.msg = msg;
    }

    @Override
    public String getCode() {
        return name();
    }

    @Override
    public String getMessage() {
        return msg;
    }
}