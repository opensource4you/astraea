package org.astraea.yunikorn.config;

import lombok.*;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
@Getter @Setter
public class Limit {

    private String limit;

    private List<String> users;

    private List<String> group;

    private Map<String, String> maxresources;
    private BigInteger maxapplications;


}
