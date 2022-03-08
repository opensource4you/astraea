package org.astraea.yunikorn.config;

import java.util.Map;
import lombok.*;
@Getter @Setter
public class Resources {
    private Map<String, String> guaranteed;
    private Map<String, String> max;
}
