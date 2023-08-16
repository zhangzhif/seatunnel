package org.apache.seatunnel.transform.groovy;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
public class GroovyTransformConfig implements Serializable {

    public static final Option<String> CODE =
            Options.key("code").stringType().noDefaultValue().withDescription("Java Code");
}
