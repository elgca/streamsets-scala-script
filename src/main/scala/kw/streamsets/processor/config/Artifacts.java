package kw.streamsets.processor.config;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Dependency;
import kw.common.data.DependencyUtils;
import org.apache.commons.lang3.StringUtils;

public class Artifacts {
    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            label = "groupId",
            description = "maven groupId",
            group = "#0",
            displayPosition = 10
    )
    public String groupId;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            label = "artifactId",
            description = "maven artifactId",
            group = "#0",
            displayPosition = 20
    )
    public String artifactId;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            label = "version",
            description = "maven version",
            group = "#0",
            displayPosition = 30
    )
    public String version;

    public DependencyUtils.MavenCoordinate get() {
        return new DependencyUtils.MavenCoordinate(groupId, artifactId, version);
    }

    public boolean noEmpty() {
        return !(StringUtils.isEmpty(groupId) ||
                StringUtils.isEmpty(artifactId) ||
                StringUtils.isEmpty(version));
    }
}
