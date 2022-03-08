package org.astraea.yunikorn.config;


import lombok.*;

import java.util.List;
@Getter @Setter
public class Fliter {
    private String type;
    private List<String> users ;
    private List<String> groups ;
}
