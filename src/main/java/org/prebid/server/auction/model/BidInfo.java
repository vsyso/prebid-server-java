package org.prebid.server.auction.model;

import com.iab.openrtb.request.Imp;
import com.iab.openrtb.response.Bid;
import lombok.Builder;
import lombok.Value;
import org.prebid.server.cache.model.CacheInfo;
import org.prebid.server.proto.openrtb.ext.response.BidType;

@Builder(toBuilder = true)
@Value
public class BidInfo {

    String generatedBidId;

    Bid bid;

    // Can be null
    Imp correspondingImp;

    String bidCurrency;

    String bidder;

    BidType bidType;

    CacheInfo cacheInfo;

    TargetingInfo targetingInfo;

    public String getBidId() {
        return generatedBidId != null ? generatedBidId : bid.getId();
    }
}
