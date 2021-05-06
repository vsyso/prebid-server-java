package org.prebid.server.bidder.criteo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Regs;
import com.iab.openrtb.response.Bid;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.criteo.model.CriteoGdprConsent;
import org.prebid.server.bidder.criteo.model.CriteoPublisher;
import org.prebid.server.bidder.criteo.model.CriteoRequest;
import org.prebid.server.bidder.criteo.model.CriteoRequestSlot;
import org.prebid.server.bidder.criteo.model.CriteoResponse;
import org.prebid.server.bidder.criteo.model.CriteoResponseSlot;
import org.prebid.server.bidder.criteo.model.CriteoUser;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.HttpCall;
import org.prebid.server.bidder.model.HttpRequest;
import org.prebid.server.bidder.model.Result;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.json.DecodeException;
import org.prebid.server.json.EncodeException;
import org.prebid.server.json.JacksonMapper;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtImpCriteo;
import org.prebid.server.proto.openrtb.ext.request.ExtRegs;
import org.prebid.server.proto.openrtb.ext.request.ExtUser;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.util.HttpUtil;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class CriteoBidder implements Bidder<CriteoRequest> {

    private static final TypeReference<ExtPrebid<?, ExtImpCriteo>> CRITEO_EXT_TYPE_REFERENCE =
            new TypeReference<ExtPrebid<?, ExtImpCriteo>>() {
            };

    private final String endpointUrl;
    private final JacksonMapper jsonMapper;
    private final CriteoSlotIdGenerator criteoSlotIdGenerator;

    public CriteoBidder(String endpointUrl, JacksonMapper jsonMapper, CriteoSlotIdGenerator criteoSlotIdGenerator) {
        this.endpointUrl = HttpUtil.validateUrl(Objects.requireNonNull(endpointUrl));
        this.jsonMapper = Objects.requireNonNull(jsonMapper);
        this.criteoSlotIdGenerator = criteoSlotIdGenerator;
    }

    @Override
    public Result<List<HttpRequest<CriteoRequest>>> makeHttpRequests(BidRequest bidRequest) {
        final CriteoRequest.CriteoRequestBuilder criteoRequestBuilder = CriteoRequest.builder().id(bidRequest.getId());

        if (CollectionUtils.isNotEmpty(bidRequest.getImp())) {
            final List<Imp> imps = bidRequest.getImp();
            final List<CriteoRequestSlot> criteoRequestSlots = new ArrayList<>();
            for (Imp imp : imps) {
                criteoRequestSlots.add(buildRequestSlot(imp));
            }

            if (allSlotsAreContainingSameNetworkId(criteoRequestSlots)) {
                return Result.withError(
                      BidderError.badInput("Bid request has slots coming with several network IDs which is not allowed")
                );
            }

            criteoRequestBuilder.slots(criteoRequestSlots);
        }

        final ExtRegs extRegs = getExtRegs(bidRequest);
        criteoRequestBuilder
                .publisher(buildCriteoPublisher(bidRequest, getNetworkId(bidRequest)))
                .user(buildCriteoUser(bidRequest, extRegs))
                .gdprConsent(buildCriteoGdprConsent(bidRequest, extRegs));

        if (bidRequest.getUser() != null && bidRequest.getUser().getExt() != null) {
            final ExtUser extUser = bidRequest.getUser().getExt();
            criteoRequestBuilder.eids(extUser.getEids());
        }

        final CriteoRequest criteoRequest = criteoRequestBuilder.build();
        final String requestBody;
        try {
            requestBody = jsonMapper.encode(criteoRequest);
        } catch (EncodeException e) {
            return Result.withError(BidderError.badInput(
                    String.format("Failed to encode request body, error: %s", e.getMessage())));
        }

        return Result.withValue(HttpRequest.<CriteoRequest>builder()
                .method(HttpMethod.POST)
                .uri(endpointUrl)
                .body(requestBody)
                .headers(resolveHeaders(criteoRequest))
                .payload(criteoRequest)
                .build());
    }

    private CriteoRequestSlot buildRequestSlot(Imp imp) {
        final CriteoRequestSlot.CriteoRequestSlotBuilder criteoRequestSlotBuilder
                = CriteoRequestSlot.builder()
                .impId(imp.getId())
                .slotId(criteoSlotIdGenerator.generateUuid())
                .sizes(getImpSizesFromBanner(imp.getBanner()));

        ExtImpCriteo extImpCriteo = parseImpExt(imp);
        if (extImpCriteo.getZoneId() != null && extImpCriteo.getZoneId() > 0) {
            criteoRequestSlotBuilder.zoneId(extImpCriteo.getZoneId());
        }

        if (extImpCriteo.getNetworkId() != null && extImpCriteo.getNetworkId() > 0) {
            criteoRequestSlotBuilder.networkId(extImpCriteo.getNetworkId());
        }
        return criteoRequestSlotBuilder.build();
    }

    private static List<String> getImpSizesFromBanner(Banner banner) {
        if (banner == null) {
            return new ArrayList<>();
        }

        final List<String> sizes = new ArrayList<>();
        if (banner.getFormat() != null) {
            for (Format format : banner.getFormat()) {
                sizes.add(String.format("%sx%s", format.getW(), format.getH()));
            }
        } else if (banner.getW() != null && banner.getW() > 0
                && banner.getH() != null && banner.getH() > 0) {
            sizes.add(String.format("%sx%s", banner.getW(), banner.getH()));
        }
        return sizes;
    }

    private ExtImpCriteo parseImpExt(Imp imp) {
        try {
            return jsonMapper.mapper().convertValue(imp.getExt(), CRITEO_EXT_TYPE_REFERENCE).getBidder();
        } catch (IllegalArgumentException e) {
            throw new PreBidException(e.getMessage());
        }
    }

    private static boolean allSlotsAreContainingSameNetworkId(List<CriteoRequestSlot> criteoRequestSlots) {
        return criteoRequestSlots.stream()
                .map(CriteoRequestSlot::getNetworkId)
                .filter(Objects::nonNull)
                .distinct()
                .count() > 1;
    }

    private static ExtRegs getExtRegs(BidRequest bidRequest) {
        final Regs regs = bidRequest.getRegs();
        return regs != null ? regs.getExt() : null;
    }

    private Integer getNetworkId(BidRequest bidRequest) {
        return bidRequest.getImp().size() > 0
                ? parseImpExt(bidRequest.getImp().get(0)).getNetworkId()
                : null;
    }

    private static CriteoPublisher buildCriteoPublisher(BidRequest bidRequest, Integer networkId) {
        final CriteoPublisher.CriteoPublisherBuilder criteoPublisherBuilder = CriteoPublisher.builder();
        if (networkId != null) {
            criteoPublisherBuilder.networkId(networkId);
        }

        if (bidRequest.getApp() != null) {
            criteoPublisherBuilder.bundleId(bidRequest.getApp().getBundle());
        }

        if (bidRequest.getSite() != null) {
            criteoPublisherBuilder
                    .siteId(bidRequest.getSite().getId())
                    .url(bidRequest.getSite().getPage());
        }

        return criteoPublisherBuilder.build();
    }

    private static CriteoUser buildCriteoUser(BidRequest bidRequest, ExtRegs extRegs) {
        final CriteoUser.CriteoUserBuilder criteoUserBuilder = CriteoUser.builder();
        if (bidRequest.getUser() != null) {
            criteoUserBuilder.cookieId(bidRequest.getUser().getBuyeruid());
        }

        if (bidRequest.getDevice() != null) {
            criteoUserBuilder
                    .deviceIdType(determineDeviceIdType(bidRequest.getDevice().getOs()))
                    .deviceOs(bidRequest.getDevice().getOs())
                    .deviceId(bidRequest.getDevice().getIfa())
                    .ip(bidRequest.getDevice().getIp())
                    .ipV6(bidRequest.getDevice().getIpv6())
                    .userAgent(bidRequest.getDevice().getUa());
        }

        if (extRegs != null) {
            criteoUserBuilder.uspIab(extRegs.getUsPrivacy());
        }

        return criteoUserBuilder.build();
    }

    private static String determineDeviceIdType(String deviceOs) {
        switch (deviceOs.toLowerCase()) {
            case "ios":
                return "idfa";
            case "android":
                return "gaid";
            default:
                return "unknown";
        }
    }

    private static CriteoGdprConsent buildCriteoGdprConsent(BidRequest bidRequest, ExtRegs extRegs) {
        final CriteoGdprConsent.CriteoGdprConsentBuilder criteoGdprConsentBuilder = CriteoGdprConsent.builder();
        if (bidRequest.getUser() != null && bidRequest.getUser().getExt() != null) {
            final ExtUser extUser = bidRequest.getUser().getExt();
            criteoGdprConsentBuilder.consentData(extUser.getConsent());
        }

        if (extRegs != null && extRegs.getGdpr() != null) {
            criteoGdprConsentBuilder.gdprApplies(extRegs.getGdpr() == 1);
        }
        return criteoGdprConsentBuilder.build();
    }

    private static MultiMap resolveHeaders(CriteoRequest criteoRequest) {
        final MultiMap headers = HttpUtil.headers();

        if (StringUtils.isNotBlank(criteoRequest.getUser().getCookieId())) {
            headers.add(HttpUtil.COOKIE_HEADER, String.format("uid=%s", criteoRequest.getUser().getCookieId()));
        }

        if (StringUtils.isNotBlank(criteoRequest.getUser().getIp())) {
            headers.add(HttpUtil.X_FORWARDED_FOR_HEADER, criteoRequest.getUser().getIp());
        }

        if (StringUtils.isNotBlank(criteoRequest.getUser().getIpV6())) {
            headers.add(HttpUtil.X_FORWARDED_FOR_HEADER, criteoRequest.getUser().getIpV6());
        }

        if (StringUtils.isNotBlank(criteoRequest.getUser().getUserAgent())) {
            headers.add(HttpUtil.USER_AGENT_HEADER, criteoRequest.getUser().getUserAgent());
        }

        return headers;
    }

    @Override
    public Result<List<BidderBid>> makeBids(HttpCall<CriteoRequest> httpCall, BidRequest bidRequest) {
        try {
            final CriteoResponse criteoResponse
                    = jsonMapper.decodeValue(httpCall.getResponse().getBody(), CriteoResponse.class);
            return Result.withValues(extractBidsFromResponse(criteoResponse));
        } catch (DecodeException e) {
            return Result.withError(BidderError.badServerResponse(e.getMessage()));
        }
    }

    private static List<BidderBid> extractBidsFromResponse(CriteoResponse criteoResponse) {
        return criteoResponse.getSlots().stream()
                .map(CriteoBidder::slotToBidderBid)
                .collect(Collectors.toList());
    }

    private static BidderBid slotToBidderBid(CriteoResponseSlot slot) {
        return BidderBid.of(slotToBid(slot), BidType.banner, slot.getCurrency());
    }

    private static Bid slotToBid(CriteoResponseSlot slot) {
        return Bid.builder()
                .id(slot.getId())
                .impid(slot.getImpId())
                .price(BigDecimal.valueOf(slot.getCpm()))
                .adm(slot.getCreative())
                .w(slot.getWidth())
                .h(slot.getHeight())
                .crid(slot.getCreativeId())
                .build();
    }

}
