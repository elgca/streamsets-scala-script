package kw.streamsets.processor;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import kw.common.data.DependencyUtils;
import kw.streamsets.processor.config.Artifacts;
import kw.streamsets.processor.config.MavenConfig;
import kw.streamsets.processor.config.ScalaScriptBean;

import java.util.stream.Collectors;

@StageDef(
        version = 1,
        label = "Scala Script",
        description = "Scala脚本工具",
        icon = "scala.png",
        producesEvents = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class ScalaScriptDProcessor extends ScalaScriptProcessor {
    @ConfigDefBean(groups = "MAVEN")
    public MavenConfig mavenConfig = new MavenConfig();

    @ConfigDefBean(groups = "SCALA")
    public ScalaScriptBean scalaScriptBean = new ScalaScriptBean();

    @Override
    public String scripts() {
        return scalaScriptBean.initScript;
    }

    @Override
    public java.util.List<DependencyUtils.MavenCoordinate> packages() {
        return mavenConfig.artifacts.stream().filter(Artifacts::noEmpty).map(Artifacts::get).collect(Collectors.toList());
    }

    @Override
    public java.util.List<String> remoteRepos() {
        return mavenConfig.remoteRepo;
    }
}
