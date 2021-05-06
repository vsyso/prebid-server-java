package org.prebid.server.bidder.criteo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class CriteoPublisher {

    @JsonProperty("siteid")
    String siteId;

    @JsonProperty("bundleid")
    String bundleId;

    String url;

    @JsonProperty("networkid")
    Integer networkId;

}
