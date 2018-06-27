package kw.streamsets.processor.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ListBeanModel;

import java.util.ArrayList;
import java.util.List;

public class MavenConfig {

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.LIST,
            defaultValue = "[\n" +
                    "\t\"http://maven.aliyun.com/nexus/content/groups/public/\"\n" +
                    "]",
            label = "仓库地址",
            description = "maven服务地址",
            displayPosition = 10,
            group = "#0"
    )
    public List<String> remoteRepo = new ArrayList<>();


    @ConfigDef(
            required = false,
            type = ConfigDef.Type.MODEL,
            defaultValue = "[]",
            label = "依赖包",
            description = "maven包管理配置",
            displayPosition = 30,
            group = "#0"
    )
    @ListBeanModel
    public java.util.List<Artifacts> artifacts = new ArrayList<>();
}

