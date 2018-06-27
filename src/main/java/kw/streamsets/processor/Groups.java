package kw.streamsets.processor;


import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

@GenerateResourceBundle
public enum Groups implements Label {
    MAVEN("maven配置"),SCALA("Scala脚本"),
    ;

    private final String label;

    private Groups(String label) {
        this.label = label;
    }

    /** {@inheritDoc} */
    @Override
    public String getLabel() {
        return this.label;
    }
}