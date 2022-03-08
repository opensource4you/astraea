package org.astraea.yunikorn.config;
import lombok.*;
@Getter @Setter
public class PlacementRule {
    private String name;
    private Boolean create ;
    private Fliter filter;
    private PlacementRule parent;
    private String value ;


}
