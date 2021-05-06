package org.prebid.server.bidder.criteo;

import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class CriteoSlotIdGenerator {

    public String generateUuid() {
        return UUID.randomUUID().toString();
    }

}
