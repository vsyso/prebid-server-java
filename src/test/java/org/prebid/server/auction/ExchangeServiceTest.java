package org.prebid.server.auction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.iab.openrtb.request.App;
import com.iab.openrtb.request.Banner;
import com.iab.openrtb.request.BidRequest;
import com.iab.openrtb.request.BidRequest.BidRequestBuilder;
import com.iab.openrtb.request.Content;
import com.iab.openrtb.request.Data;
import com.iab.openrtb.request.Device;
import com.iab.openrtb.request.Format;
import com.iab.openrtb.request.Geo;
import com.iab.openrtb.request.Imp;
import com.iab.openrtb.request.Imp.ImpBuilder;
import com.iab.openrtb.request.Publisher;
import com.iab.openrtb.request.Regs;
import com.iab.openrtb.request.Site;
import com.iab.openrtb.request.User;
import com.iab.openrtb.request.Video;
import com.iab.openrtb.response.Bid;
import com.iab.openrtb.response.BidResponse;
import com.iab.openrtb.response.SeatBid;
import io.vertx.core.Future;
import org.apache.commons.collections4.MapUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.auction.model.AuctionContext;
import org.prebid.server.auction.model.BidRequestCacheInfo;
import org.prebid.server.auction.model.BidderPrivacyResult;
import org.prebid.server.auction.model.BidderRequest;
import org.prebid.server.auction.model.BidderResponse;
import org.prebid.server.auction.model.MultiBidConfig;
import org.prebid.server.auction.model.StoredResponseResult;
import org.prebid.server.bidder.Bidder;
import org.prebid.server.bidder.BidderCatalog;
import org.prebid.server.bidder.HttpBidderRequester;
import org.prebid.server.bidder.Usersyncer;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderError;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.cookie.UidsCookie;
import org.prebid.server.currency.CurrencyConversionService;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.exception.PreBidException;
import org.prebid.server.execution.Timeout;
import org.prebid.server.execution.TimeoutFactory;
import org.prebid.server.metric.MetricName;
import org.prebid.server.metric.Metrics;
import org.prebid.server.proto.openrtb.ext.ExtPrebid;
import org.prebid.server.proto.openrtb.ext.request.BidAdjustmentMediaType;
import org.prebid.server.proto.openrtb.ext.request.ExtApp;
import org.prebid.server.proto.openrtb.ext.request.ExtBidderConfig;
import org.prebid.server.proto.openrtb.ext.request.ExtBidderConfigOrtb;
import org.prebid.server.proto.openrtb.ext.request.ExtGranularityRange;
import org.prebid.server.proto.openrtb.ext.request.ExtPriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtRegs;
import org.prebid.server.proto.openrtb.ext.request.ExtRequest;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestBidadjustmentfactors;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestCurrency;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidBidderConfig;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidCache;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidCacheBids;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidCacheVastxml;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidData;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidDataEidPermissions;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidMultiBid;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidSchain;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidSchainSchain;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestPrebidSchainSchainNode;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestTargeting;
import org.prebid.server.proto.openrtb.ext.request.ExtSite;
import org.prebid.server.proto.openrtb.ext.request.ExtUser;
import org.prebid.server.proto.openrtb.ext.request.ExtUserEid;
import org.prebid.server.proto.openrtb.ext.request.ExtUserPrebid;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebid;
import org.prebid.server.proto.openrtb.ext.response.ExtBidResponse;
import org.prebid.server.proto.openrtb.ext.response.ExtBidderError;
import org.prebid.server.proto.openrtb.ext.response.ExtHttpCall;
import org.prebid.server.settings.model.Account;
import org.prebid.server.validation.ResponseBidValidator;
import org.prebid.server.validation.model.ValidationResult;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.prebid.server.proto.openrtb.ext.response.BidType.banner;
import static org.prebid.server.proto.openrtb.ext.response.BidType.video;

public class ExchangeServiceTest extends VertxTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private BidderCatalog bidderCatalog;
    @Mock
    private StoredResponseProcessor storedResponseProcessor;
    @Mock
    private PrivacyEnforcementService privacyEnforcementService;
    @Mock
    private FpdResolver fpdResolver;
    @Mock
    private SchainResolver schainResolver;
    @Mock
    private HttpBidderRequester httpBidderRequester;
    @Mock
    private ResponseBidValidator responseBidValidator;
    @Mock
    private CurrencyConversionService currencyService;
    @Mock
    private BidResponseCreator bidResponseCreator;
    @Spy
    private BidResponsePostProcessor.NoOpBidResponsePostProcessor bidResponsePostProcessor;
    @Mock
    private Metrics metrics;
    @Mock
    private UidsCookie uidsCookie;

    private Clock clock;

    private ExchangeService exchangeService;

    private Timeout timeout;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        given(bidResponseCreator.create(anyList(), any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenBidResponseWithBids(singletonList(givenBid(identity())))));

        given(bidderCatalog.isValidName(anyString())).willReturn(true);
        given(bidderCatalog.isActive(anyString())).willReturn(true);
        given(bidderCatalog.usersyncerByName(anyString())).willReturn(Usersyncer.of("cookieFamily", null, null));

        given(privacyEnforcementService.mask(any(), argThat(MapUtils::isNotEmpty), any(), any()))
                .willAnswer(inv ->
                        Future.succeededFuture(((Map<String, User>) inv.getArgument(1)).entrySet().stream()
                                .map(bidderAndUser -> BidderPrivacyResult.builder()
                                        .requestBidder(bidderAndUser.getKey())
                                        .user(bidderAndUser.getValue())
                                        .build())
                                .collect(Collectors.toList())));

        given(privacyEnforcementService.mask(any(), argThat(MapUtils::isEmpty), any(), any()))
                .willReturn(Future.succeededFuture(emptyList()));

        given(fpdResolver.resolveUser(any(), any())).willAnswer(invocation -> invocation.getArgument(0));
        given(fpdResolver.resolveSite(any(), any())).willAnswer(invocation -> invocation.getArgument(0));
        given(fpdResolver.resolveApp(any(), any())).willAnswer(invocation -> invocation.getArgument(0));
        given(fpdResolver.resolveImpExt(any(), anyBoolean()))
                .willAnswer(invocation -> invocation.getArgument(0));

        given(schainResolver.resolveForBidder(anyString(), any())).willReturn(null);

        given(responseBidValidator.validate(any(), any(), any(), any())).willReturn(ValidationResult.success());

        given(currencyService.convertCurrency(any(), any(), any(), any()))
                .willAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        given(storedResponseProcessor.getStoredResponseResult(any(), any()))
                .willAnswer(inv -> Future.succeededFuture(StoredResponseResult.of(inv.getArgument(0), emptyList(),
                        emptyMap())));
        given(storedResponseProcessor.mergeWithBidderResponses(any(), any(), any())).willAnswer(
                inv -> inv.getArgument(0));

        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        timeout = new TimeoutFactory(clock).create(500);

        exchangeService = new ExchangeService(
                0,
                bidderCatalog,
                storedResponseProcessor,
                privacyEnforcementService,
                fpdResolver,
                schainResolver,
                httpBidderRequester,
                responseBidValidator,
                currencyService,
                bidResponseCreator,
                bidResponsePostProcessor,
                metrics,
                clock,
                jacksonMapper);
    }

    @Test
    public void creationShouldFailOnNegativeExpectedCacheTime() {
        assertThatIllegalArgumentException().isThrownBy(
                () -> new ExchangeService(
                        -1,
                        bidderCatalog,
                        storedResponseProcessor,
                        privacyEnforcementService,
                        fpdResolver,
                        schainResolver,
                        httpBidderRequester,
                        responseBidValidator,
                        currencyService,
                        bidResponseCreator,
                        bidResponsePostProcessor,
                        metrics,
                        clock,
                        jacksonMapper));
    }

    @Test
    public void shouldTolerateImpWithoutExtension() {
        // given
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(null));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verifyZeroInteractions(bidderCatalog);
        verifyZeroInteractions(httpBidderRequester);
        assertThat(bidResponse).isNotNull();
    }

    @Test
    public void shouldTolerateImpWithUnknownBidderInExtension() {
        // given
        given(bidderCatalog.isValidName(anyString())).willReturn(false);

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("invalid", 0)));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verify(bidderCatalog).isValidName(eq("invalid"));
        verifyZeroInteractions(httpBidderRequester);
        assertThat(bidResponse).isNotNull();
    }

    @Test
    public void shouldTolerateMissingPrebidImpExtension() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final BidRequest capturedBidRequest = captureBidRequest();
        assertThat(capturedBidRequest.getImp()).hasSize(1)
                .element(0)
                .returns(mapper.valueToTree(ExtPrebid.of(null, 1)), Imp::getExt);
    }

    @Test
    public void shouldExtractRequestWithBidderSpecificExtension() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(singletonMap("someBidder", 1), builder -> builder
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder.id("requestId").tmax(500L));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final BidRequest capturedBidRequest = captureBidRequest();
        assertThat(capturedBidRequest).isEqualTo(BidRequest.builder()
                .id("requestId")
                .cur(singletonList("USD"))
                .imp(singletonList(Imp.builder()
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null, 1)))
                        .build()))
                .tmax(500L)
                .build());
    }

    @Test
    public void shouldExtractRequestWithCurrencyRatesExtension() {
        // given
        givenBidder(givenEmptySeatBid());

        final Map<String, Map<String, BigDecimal>> currencyRates = doubleMap(
                "GBP", singletonMap("EUR", BigDecimal.valueOf(1.15)),
                "UAH", singletonMap("EUR", BigDecimal.valueOf(1.1565)));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(singletonMap("someBidder", 1), builder -> builder
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder
                        .id("requestId")
                        .ext(ExtRequest.of(
                                ExtRequestPrebid.builder()
                                        .currency(ExtRequestCurrency.of(currencyRates, false))
                                        .build()))
                        .tmax(500L));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final BidRequest capturedBidRequest = captureBidRequest();
        assertThat(capturedBidRequest).isEqualTo(BidRequest.builder()
                .id("requestId")
                .cur(singletonList("USD"))
                .imp(singletonList(Imp.builder()
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null, 1)))
                        .build()))
                .ext(ExtRequest.of(
                        ExtRequestPrebid.builder().currency(ExtRequestCurrency.of(currencyRates, false)).build()))
                .tmax(500L)
                .build());
    }

    @Test
    public void shouldExtractMultipleRequests() {
        // given
        final Bidder<?> bidder1 = mock(Bidder.class);
        final Bidder<?> bidder2 = mock(Bidder.class);
        givenBidder("bidder1", bidder1, givenEmptySeatBid());
        givenBidder("bidder2", bidder2, givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(asList(
                givenImp(doubleMap("bidder1", 1, "bidder2", 2), identity()),
                givenImp(singletonMap("bidder1", 3), identity())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequest1Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder1), bidRequest1Captor.capture(), any(), anyBoolean());
        final BidderRequest capturedBidRequest1 = bidRequest1Captor.getValue();
        assertThat(capturedBidRequest1.getBidRequest().getImp()).hasSize(2)
                .extracting(imp -> imp.getExt().get("bidder").asInt())
                .containsOnly(1, 3);

        final ArgumentCaptor<BidderRequest> bidRequest2Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder2), bidRequest2Captor.capture(), any(), anyBoolean());
        final BidderRequest capturedBidRequest2 = bidRequest2Captor.getValue();
        assertThat(capturedBidRequest2.getBidRequest().getImp()).hasSize(1)
                .element(0).returns(2, imp -> imp.getExt().get("bidder").asInt());
    }

    @Test
    public void shouldPassRequestWithExtPrebidToDefinedBidder() {
        // given
        final String bidder1Name = "bidder1";
        final String bidder2Name = "bidder2";
        final Bidder<?> bidder1 = mock(Bidder.class);
        final Bidder<?> bidder2 = mock(Bidder.class);
        givenBidder(bidder1Name, bidder1, givenEmptySeatBid());
        givenBidder(bidder2Name, bidder2, givenEmptySeatBid());

        final ExtRequest extRequest = ExtRequest.of(
                ExtRequestPrebid.builder()
                        .bidders(mapper.createObjectNode()
                                .putPOJO(bidder1Name, mapper.createObjectNode().put("test1", "test1"))
                                .putPOJO(bidder2Name, mapper.createObjectNode().put("test2", "test2"))
                                .putPOJO("spam", mapper.createObjectNode().put("spam", "spam")))
                        .auctiontimestamp(1000L)
                        .build());

        final BidRequest bidRequest = givenBidRequest(asList(
                givenImp(singletonMap(bidder1Name, 1), identity()),
                givenImp(singletonMap(bidder2Name, 2), identity())),
                builder -> builder.ext(extRequest));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequest1Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder1), bidRequest1Captor.capture(), any(), anyBoolean());

        final BidderRequest capturedBidRequest1 = bidRequest1Captor.getValue();
        final ExtRequestPrebid prebid1 = capturedBidRequest1.getBidRequest().getExt().getPrebid();
        assertThat(prebid1).isNotNull();
        final JsonNode bidders1 = prebid1.getBidders();
        assertThat(bidders1).isNotNull();
        assertThat(bidders1.fields()).hasSize(1)
                .containsOnly(entry("bidder", mapper.createObjectNode().put("test1", "test1")));

        final ArgumentCaptor<BidderRequest> bidRequest2Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder2), bidRequest2Captor.capture(), any(), anyBoolean());
        final BidRequest capturedBidRequest2 = bidRequest2Captor.getValue().getBidRequest();
        final ExtRequestPrebid prebid2 = capturedBidRequest2.getExt().getPrebid();
        assertThat(prebid2).isNotNull();
        final JsonNode bidders2 = prebid2.getBidders();
        assertThat(bidders2).isNotNull();
        assertThat(bidders2.fields()).hasSize(1)
                .containsOnly(entry("bidder", mapper.createObjectNode().put("test2", "test2")));
    }

    @Test
    public void shouldPassRequestWithInjectedSchainInSourceExt() {
        // given
        final String bidder1Name = "bidder1";
        final String bidder2Name = "bidder2";
        final String bidder3Name = "bidder3";
        final Bidder<?> bidder1 = mock(Bidder.class);
        final Bidder<?> bidder2 = mock(Bidder.class);
        final Bidder<?> bidder3 = mock(Bidder.class);
        givenBidder(bidder1Name, bidder1, givenEmptySeatBid());
        givenBidder(bidder2Name, bidder2, givenEmptySeatBid());
        givenBidder(bidder3Name, bidder3, givenEmptySeatBid());

        final ExtRequestPrebidSchainSchainNode specificNodes = ExtRequestPrebidSchainSchainNode.of(
                "asi", "sid", 1, "rid", "name", "domain", null);
        final ExtRequestPrebidSchainSchain specificSchain = ExtRequestPrebidSchainSchain.of(
                "ver", 1, singletonList(specificNodes), null);
        final ExtRequestPrebidSchain schainForBidders = ExtRequestPrebidSchain.of(
                asList(bidder1Name, bidder2Name), specificSchain);

        final ExtRequestPrebidSchainSchainNode generalNodes = ExtRequestPrebidSchainSchainNode.of(
                "t", null, 0, "a", null, "ads", null);
        final ExtRequestPrebidSchainSchain generalSchain = ExtRequestPrebidSchainSchain.of(
                "t", 123, singletonList(generalNodes), null);
        final ExtRequestPrebidSchain allSchain = ExtRequestPrebidSchain.of(singletonList("*"), generalSchain);

        final ExtRequest extRequest = ExtRequest.of(
                ExtRequestPrebid.builder()
                        .schains(asList(schainForBidders, allSchain))
                        .auctiontimestamp(1000L)
                        .build());
        final BidRequest bidRequest = givenBidRequest(
                asList(
                        givenImp(singletonMap(bidder1Name, 1), identity()),
                        givenImp(singletonMap(bidder2Name, 2), identity()),
                        givenImp(singletonMap(bidder3Name, 3), identity())),
                builder -> builder.ext(extRequest));

        given(schainResolver.resolveForBidder(eq("bidder1"), same(bidRequest))).willReturn(specificSchain);
        given(schainResolver.resolveForBidder(eq("bidder2"), same(bidRequest))).willReturn(specificSchain);
        given(schainResolver.resolveForBidder(eq("bidder3"), same(bidRequest))).willReturn(generalSchain);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequest1Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder1), bidRequest1Captor.capture(), any(), anyBoolean());
        final BidRequest capturedBidRequest1 = bidRequest1Captor.getValue().getBidRequest();
        final ExtRequestPrebidSchainSchain requestSchain1 = capturedBidRequest1.getSource().getExt().getSchain();
        assertThat(requestSchain1).isNotNull();
        assertThat(requestSchain1).isEqualTo(specificSchain);
        assertThat(capturedBidRequest1.getExt().getPrebid().getSchains()).isNull();

        final ArgumentCaptor<BidderRequest> bidRequest2Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder2), bidRequest2Captor.capture(), any(), anyBoolean());
        final BidRequest capturedBidRequest2 = bidRequest2Captor.getValue().getBidRequest();
        final ExtRequestPrebidSchainSchain requestSchain2 = capturedBidRequest2.getSource().getExt().getSchain();
        assertThat(requestSchain2).isNotNull();
        assertThat(requestSchain2).isEqualTo(specificSchain);
        assertThat(capturedBidRequest2.getExt().getPrebid().getSchains()).isNull();

        final ArgumentCaptor<BidderRequest> bidRequest3Captor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder3), bidRequest3Captor.capture(), any(), anyBoolean());
        final BidRequest capturedBidRequest3 = bidRequest3Captor.getValue().getBidRequest();
        final ExtRequestPrebidSchainSchain requestSchain3 = capturedBidRequest3.getSource().getExt().getSchain();
        assertThat(requestSchain3).isNotNull();
        assertThat(requestSchain3).isEqualTo(generalSchain);
        assertThat(capturedBidRequest3.getExt().getPrebid().getSchains()).isNull();
    }

    @Test
    public void shouldReturnFailedFutureWithUnchangedMessageWhenPrivacyEnforcementServiceFails() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());

        given(privacyEnforcementService.mask(any(), any(), any(), any()))
                .willReturn(Future.failedFuture("Error when retrieving allowed purpose ids"));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                bidRequestBuilder -> bidRequestBuilder
                        .regs(Regs.of(null, ExtRegs.of(1, null))));

        // when
        final Future<?> result = exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        assertThat(result.failed()).isTrue();
        assertThat(result.cause()).hasMessage("Error when retrieving allowed purpose ids");
    }

    @Test
    public void shouldNotCreateRequestForBidderRestrictedByPrivacyEnforcement() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenEmptySeatBid());

        final BidderPrivacyResult restrictedPrivacy = BidderPrivacyResult.builder()
                .requestBidder("bidderAlias")
                .blockedRequestByTcf(true)
                .build();
        given(privacyEnforcementService.mask(any(), any(), any(), any()))
                .willReturn(Future.succeededFuture(singletonList(restrictedPrivacy)));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(singletonMap("bidderAlias", 1), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(singletonMap("bidderAlias", "bidder"))
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verifyZeroInteractions(httpBidderRequester);
    }

    @Test
    public void shouldExtractRequestByAliasForCorrectBidder() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(singletonMap("bidderAlias", 1), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(singletonMap("bidderAlias", "bidder"))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(same(bidder), bidRequestCaptor.capture(), any(), anyBoolean());
        assertThat(bidRequestCaptor.getValue().getBidRequest().getImp()).hasSize(1)
                .extracting(imp -> imp.getExt().get("bidder").asInt())
                .contains(1);
    }

    @Test
    public void shouldExtractMultipleRequestsForTheSameBidderIfAliasesWereUsed() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(doubleMap("bidder", 1, "bidderAlias", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(singletonMap("bidderAlias", "bidder"))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(same(bidder), bidRequestCaptor.capture(), any(),
                anyBoolean());
        final List<BidderRequest> capturedBidderRequests = bidRequestCaptor.getAllValues();

        assertThat(capturedBidderRequests).hasSize(2)
                .extracting(BidderRequest::getBidRequest)
                .extracting(capturedBidRequest -> capturedBidRequest.getImp().get(0).getExt().get("bidder").asInt())
                .containsOnly(2, 1);
    }

    @Test
    public void shouldTolerateBidderResultWithoutBids() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        givenBidResponseCreator(emptyMap());

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid()).isEmpty();
    }

    @Test
    public void shouldReturnSeparateSeatBidsForTheSameBidderIfBiddersAliasAndBidderWereUsedWithinSingleImp() {
        // given
        given(httpBidderRequester.requestBids(any(),
                eq(BidderRequest.of("bidder", null, givenBidRequest(
                        singletonList(givenImp(
                                null,
                                builder -> builder.ext(mapper.valueToTree(
                                        ExtPrebid.of(null, 1))))),
                        builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .auctiontimestamp(1000L)
                                .aliases(singletonMap("bidderAlias", "bidder")).build()))))), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(BigDecimal.ONE).build())))));

        given(httpBidderRequester.requestBids(any(),
                eq(BidderRequest.of("bidderAlias", null, givenBidRequest(
                        singletonList(givenImp(
                                null,
                                builder -> builder.ext(mapper.valueToTree(
                                        ExtPrebid.of(null, 2))))),
                        builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .auctiontimestamp(1000L)
                                .aliases(singletonMap("bidderAlias", "bidder")).build()))))), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(BigDecimal.ONE).build())))));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(doubleMap("bidder", 1, "bidderAlias", 2)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(singletonMap("bidderAlias", "bidder"))
                        .auctiontimestamp(1000L)
                        .build())));

        given(bidResponseCreator.create(anyList(), any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(BidResponse.builder()
                        .seatbid(asList(
                                givenSeatBid(singletonList(givenBid(identity())), identity()),
                                givenSeatBid(singletonList(givenBid(identity())), identity())))
                        .build()));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verify(httpBidderRequester, times(2)).requestBids(any(), any(), any(), anyBoolean());
        assertThat(bidResponse.getSeatbid()).hasSize(2)
                .extracting(seatBid -> seatBid.getBid().size())
                .containsOnly(1, 1);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldCallBidResponseCreatorWithExpectedParamsAndUpdateDebugErrors() {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenEmptySeatBid());

        final Bid thirdBid = Bid.builder().id("bidId3").impid("impId1").price(BigDecimal.valueOf(7.89)).build();
        givenBidder("bidder2", mock(Bidder.class), givenSeatBid(singletonList(givenBid(thirdBid))));

        final ExtRequestPrebidMultiBid multiBid1 = ExtRequestPrebidMultiBid.of("bidder1", null, 2, "bi1");
        final ExtRequestPrebidMultiBid multiBid2 = ExtRequestPrebidMultiBid.of("bidder2", singletonList("invalid"), 4,
                "bi2");
        final ExtRequestPrebidMultiBid multiBid3 = ExtRequestPrebidMultiBid.of("bidder3", singletonList("invalid"),
                null, "bi3");
        final ExtRequestPrebidMultiBid duplicateMultiBid1 = ExtRequestPrebidMultiBid.of("bidder1", null, 100, "bi1_2");
        final ExtRequestPrebidMultiBid duplicateMultiBids1 = ExtRequestPrebidMultiBid.of(null, singletonList("bidder1"),
                100, "bi1_3");
        final ExtRequestPrebidMultiBid multiBid4 = ExtRequestPrebidMultiBid.of(null,
                Arrays.asList("bidder4", "bidder5"), 3, "ignored");

        final ExtRequestTargeting targeting = givenTargeting(true);
        final ObjectNode events = mapper.createObjectNode();
        final BidRequest bidRequest = givenBidRequest(asList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1")),
                givenImp(doubleMap("bidder1", 1, "bidder2", 2), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .targeting(targeting)
                        .auctiontimestamp(1000L)
                        .events(events)
                        .multibid(Arrays.asList(multiBid1, multiBid2, multiBid3, duplicateMultiBid1,
                                duplicateMultiBids1, multiBid4))
                        .cache(ExtRequestPrebidCache.of(ExtRequestPrebidCacheBids.of(53, true),
                                ExtRequestPrebidCacheVastxml.of(34, true), true))
                        .build()))
        );
        final AuctionContext auctionContext = givenRequestContext(bidRequest);

        // when
        exchangeService.holdAuction(auctionContext).result();

        // then
        final BidRequestCacheInfo expectedCacheInfo = BidRequestCacheInfo.builder()
                .doCaching(true)
                .shouldCacheBids(true)
                .shouldCacheVideoBids(true)
                .returnCreativeBids(true)
                .returnCreativeVideoBids(true)
                .cacheBidsTtl(53)
                .cacheVideoBidsTtl(34)
                .shouldCacheWinningBidsOnly(false)
                .build();

        final MultiBidConfig expectedMultiBid1 = MultiBidConfig.of(multiBid1.getBidder(), multiBid1.getMaxBids(),
                multiBid1.getTargetBidderCodePrefix());
        final MultiBidConfig expectedMultiBid2 = MultiBidConfig.of(multiBid2.getBidder(), multiBid2.getMaxBids(),
                multiBid2.getTargetBidderCodePrefix());
        final MultiBidConfig expectedFirstMultiBid4 = MultiBidConfig.of("bidder4", multiBid4.getMaxBids(), null);
        final MultiBidConfig expectedSecondMultiBid4 = MultiBidConfig.of("bidder5", multiBid4.getMaxBids(), null);

        final Map<String, MultiBidConfig> expectedMultiBidMap = new HashMap<>();
        expectedMultiBidMap.put(expectedMultiBid1.getBidder(), expectedMultiBid1);
        expectedMultiBidMap.put(expectedMultiBid2.getBidder(), expectedMultiBid2);
        expectedMultiBidMap.put(expectedFirstMultiBid4.getBidder(), expectedFirstMultiBid4);
        expectedMultiBidMap.put(expectedSecondMultiBid4.getBidder(), expectedSecondMultiBid4);

        final AuctionContext expectedAuctionContext = auctionContext.toBuilder()
                .debugWarnings(asList(
                        "Invalid MultiBid: bidder bidder2 and bidders [invalid] specified."
                                + " Only bidder bidder2 will be used.",
                        "Invalid MultiBid: bidder bidder3 and bidders [invalid] specified."
                                + " Only bidder bidder3 will be used.",
                        "Invalid MultiBid: MaxBids for bidder bidder3 is not specified and will be skipped.",
                        "Invalid MultiBid: Bidder bidder1 specified multiple times.",
                        "Invalid MultiBid: CodePrefix bi1_3 that was specified for bidders [bidder1] will be skipped.",
                        "Invalid MultiBid: Bidder bidder1 specified multiple times.",
                        "Invalid MultiBid: CodePrefix ignored that was specified for bidders [bidder4, bidder5]"
                                + " will be skipped."))
                .build();

        final ArgumentCaptor<List<BidderResponse>> captor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(captor.capture(), eq(expectedAuctionContext), eq(expectedCacheInfo),
                eq(expectedMultiBidMap), eq(false));

        final ObjectNode expectedBidExt = mapper.createObjectNode().put("origbidcpm", new BigDecimal("7.89"));
        final Bid expectedThirdBid = Bid.builder()
                .id("bidId3")
                .impid("impId1")
                .price(BigDecimal.valueOf(7.89))
                .ext(expectedBidExt)
                .build();
        assertThat(captor.getValue()).containsOnly(
                BidderResponse.of("bidder2", BidderSeatBid.of(singletonList(
                        BidderBid.of(expectedThirdBid, banner, null)), emptyList(), emptyList()), 0),
                BidderResponse.of("bidder1", BidderSeatBid.of(emptyList(), emptyList(), emptyList()), 0));
    }

    @Test
    public void shouldCallBidResponseCreatorWithWinningOnlyTrueWhenIncludeBidderKeysIsFalse() {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenEmptySeatBid());

        final Bid thirdBid = Bid.builder().id("bidId3").impid("impId1").price(BigDecimal.valueOf(7.89)).build();
        givenBidder("bidder2", mock(Bidder.class), givenSeatBid(singletonList(givenBid(thirdBid))));

        final ExtRequestTargeting targeting = givenTargeting(false);

        final BidRequest bidRequest = givenBidRequest(asList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1")),
                givenImp(doubleMap("bidder1", 1, "bidder2", 2), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .targeting(targeting)
                        .cache(ExtRequestPrebidCache.of(null, null, true))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<AuctionContext> auctionContextArgumentCaptor =
                ArgumentCaptor.forClass(AuctionContext.class);
        verify(bidResponseCreator).create(
                anyList(),
                auctionContextArgumentCaptor.capture(),
                eq(BidRequestCacheInfo.builder().doCaching(true).shouldCacheWinningBidsOnly(true).build()),
                eq(emptyMap()),
                eq(false));

        assertThat(singletonList(auctionContextArgumentCaptor.getValue().getBidRequest()))
                .extracting(BidRequest::getExt)
                .extracting(ExtRequest::getPrebid)
                .extracting(ExtRequestPrebid::getCache)
                .extracting(ExtRequestPrebidCache::getWinningonly)
                .containsOnly(true);
    }

    @Test
    public void shouldCallBidResponseCreatorWithWinningOnlyFalseWhenWinningOnlyIsNull() {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenEmptySeatBid());

        final Bid thirdBid = Bid.builder().id("bidId3").impid("impId1").price(BigDecimal.valueOf(7.89)).build();
        givenBidder("bidder2", mock(Bidder.class), givenSeatBid(singletonList(givenBid(thirdBid))));

        final ExtRequestTargeting targeting = givenTargeting(false);

        final BidRequest bidRequest = givenBidRequest(asList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1")),
                givenImp(doubleMap("bidder1", 1, "bidder2", 2), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .targeting(targeting)
                        .cache(ExtRequestPrebidCache.of(null, null, null))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verify(bidResponseCreator).create(
                anyList(),
                any(),
                eq(BidRequestCacheInfo.builder().build()),
                eq(emptyMap()),
                anyBoolean());
    }

    @Test
    public void shouldCallBidResponseCreatorWithEnabledDebugTrueIfTestFlagIsTrue() {
        // given
        givenBidder("bidder1", mock(Bidder.class), BidderSeatBid.of(
                singletonList(givenBid(Bid.builder().price(BigDecimal.ONE).build())),
                singletonList(ExtHttpCall.builder()
                        .uri("bidder1_uri1")
                        .requestbody("bidder1_requestBody1")
                        .status(200)
                        .responsebody("bidder1_responseBody1")
                        .build()),
                emptyList()));

        final BidRequest bidRequest = givenBidRequest(
                givenSingleImp(singletonMap("bidder1", 1)),
                builder -> builder.test(1));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verify(bidResponseCreator).create(anyList(), any(), any(), eq(emptyMap()), eq(true));
    }

    @Test
    public void shouldCallBidResponseCreatorWithEnabledDebugTrueIfExtPrebidDebugIsOn() {
        // given
        givenBidder("bidder1", mock(Bidder.class), BidderSeatBid.of(
                singletonList(givenBid(Bid.builder().price(BigDecimal.ONE).build())),
                singletonList(ExtHttpCall.builder()
                        .uri("bidder1_uri1")
                        .requestbody("bidder1_requestBody1")
                        .status(200)
                        .responsebody("bidder1_responseBody1")
                        .build()),
                emptyList()));

        final BidRequest bidRequest = givenBidRequest(
                givenSingleImp(singletonMap("bidder1", 1)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .debug(1)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        verify(bidResponseCreator).create(anyList(), any(), any(), any(), eq(true));
    }

    @Test
    public void shouldTolerateNullRequestExtPrebid() {
        // given
        givenBidder(givenSingleSeatBid(givenBid(Bid.builder().price(BigDecimal.ONE).build())));

        final BidRequest bidRequest = givenBidRequest(
                givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.ext(jacksonMapper.fillExtension(ExtRequest.empty(), singletonMap("someField", 1))));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid()).flatExtracting(SeatBid::getBid)
                .extracting(bid -> toExtPrebid(bid.getExt()).getPrebid().getTargeting())
                .allSatisfy(map -> assertThat(map).isNull());
    }

    @Test
    public void shouldTolerateNullRequestExtPrebidTargeting() {
        // given
        givenBidder(givenSingleSeatBid(givenBid(Bid.builder().price(BigDecimal.ONE).build())));

        final BidRequest bidRequest = givenBidRequest(
                givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid()).flatExtracting(SeatBid::getBid)
                .extracting(bid -> toExtPrebid(bid.getExt()).getPrebid().getTargeting())
                .allSatisfy(map -> assertThat(map).isNull());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTolerateResponseBidValidationErrors() {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().id("bidId1").impid("impId1").price(BigDecimal.valueOf(1.23)).build()))));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .auctiontimestamp(1000L)
                        .build())));

        given(responseBidValidator.validate(any(), any(), any(), any())).willReturn(ValidationResult.error(
                singletonList("bid validation warning"),
                "bid validation error"));

        givenBidResponseCreator(singletonList(Bid.builder().build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> bidderResponsesCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(bidderResponsesCaptor.capture(), any(), any(), any(), anyBoolean());
        final List<BidderResponse> bidderResponses = bidderResponsesCaptor.getValue();

        assertThat(bidderResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .isEmpty();
        assertThat(bidderResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getErrors)
                .containsOnly(
                        BidderError.generic("bid validation warning"),
                        BidderError.generic("bid validation error"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldTolerateResponseBidValidationWarnings() {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().id("bidId1").impid("impId1").price(BigDecimal.valueOf(1.23)).build()))));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .auctiontimestamp(1000L)
                        .build())));

        given(responseBidValidator.validate(any(), any(), any(), any())).willReturn(ValidationResult.success(
                singletonList("bid validation warning")));

        givenBidResponseCreator(singletonList(Bid.builder().build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> bidderResponsesCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(bidderResponsesCaptor.capture(), any(), any(), any(), anyBoolean());
        final List<BidderResponse> bidderResponses = bidderResponsesCaptor.getValue();

        assertThat(bidderResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .hasSize(1);
        assertThat(bidderResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getErrors)
                .containsOnly(BidderError.generic("bid validation warning"));
    }

    @Test
    public void shouldRejectBidIfCurrencyIsNotValid() throws JsonProcessingException {
        // given
        givenBidder("bidder1", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().id("bidId1").impid("impId1").price(BigDecimal.valueOf(1.23)).build(),
                        "USDD"))));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .auctiontimestamp(1000L)
                        .build())));

        given(responseBidValidator.validate(any(), any(), any(), any()))
                .willReturn(ValidationResult.error("BidResponse currency is not valid: USDD"));

        final List<ExtBidderError> bidderErrors = singletonList(ExtBidderError.of(BidderError.Type.generic.getCode(),
                "BidResponse currency is not valid: USDD"));
        givenBidResponseCreator(singletonMap("bidder1", bidderErrors));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ExtBidResponse ext = mapper.treeToValue(bidResponse.getExt(), ExtBidResponse.class);
        assertThat(ext.getErrors()).hasSize(1)
                .containsOnly(entry("bidder1", bidderErrors));
        assertThat(bidResponse.getSeatbid())
                .extracting(SeatBid::getBid)
                .isEmpty();
    }

    @Test
    public void shouldCreateRequestsFromImpsReturnedByStoredResponseProcessor() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(asList(
                givenImp(singletonMap("someBidder1", 1), builder -> builder
                        .id("impId1")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())),
                givenImp(singletonMap("someBidder2", 1), builder -> builder
                        .id("impId2")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder.id("requestId").tmax(500L));

        given(storedResponseProcessor.getStoredResponseResult(any(), any()))
                .willReturn(Future.succeededFuture(StoredResponseResult
                        .of(singletonList(givenImp(singletonMap("someBidder1", 1), builder -> builder
                                .id("impId1")
                                .banner(Banner.builder()
                                        .format(singletonList(Format.builder().w(400).h(300).build()))
                                        .build()))), emptyList(), emptyMap())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final BidRequest capturedBidRequest = captureBidRequest();
        assertThat(capturedBidRequest).isEqualTo(BidRequest.builder()
                .id("requestId")
                .cur(singletonList("USD"))
                .imp(singletonList(Imp.builder()
                        .id("impId1")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())
                        .ext(mapper.valueToTree(ExtPrebid.of(null, 1)))
                        .build()))
                .tmax(500L)
                .build());
    }

    @Test
    public void shouldProcessBidderResponseReturnedFromStoredResponseProcessor() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(doubleMap("prebid", 0, "someBidder", 1), builder -> builder
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder.id("requestId").tmax(500L));

        given(storedResponseProcessor.mergeWithBidderResponses(any(), any(), any()))
                .willReturn(singletonList(BidderResponse.of(
                        "someBidder",
                        BidderSeatBid.of(
                                singletonList(BidderBid.of(
                                        Bid.builder().id("bidId1").price(ONE).build(), BidType.banner, "USD")),
                                null,
                                emptyList()),
                        100)));

        givenBidResponseCreator(singletonList(Bid.builder().id("bidId1").build()));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid())
                .flatExtracting(SeatBid::getBid)
                .extracting(Bid::getId)
                .containsOnly("bidId1");
    }

    @Test
    public void shouldReturnFailedFutureWhenStoredResponseProcessorGetStoredResultReturnsFailedFuture() {
        // given
        given(storedResponseProcessor.getStoredResponseResult(any(), any()))
                .willReturn(Future.failedFuture(new InvalidRequestException("Error")));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(doubleMap("prebid", 0, "someBidder", 1), builder -> builder
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder.id("requestId").tmax(500L));

        // when
        final Future<BidResponse> result = exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        assertThat(result.failed()).isTrue();
        assertThat(result.cause()).isInstanceOf(InvalidRequestException.class).hasMessage("Error");
    }

    @Test
    public void shouldReturnFailedFutureWhenStoredResponseProcessorMergeBidderResponseReturnsFailedFuture() {
        // given
        givenBidder(givenEmptySeatBid());

        given(storedResponseProcessor.mergeWithBidderResponses(any(), any(), any()))
                .willThrow(new PreBidException("Error"));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                givenImp(doubleMap("prebid", 0, "someBidder", 1), builder -> builder
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()))),
                builder -> builder.id("requestId").tmax(500L));

        // when
        final Future<BidResponse> result = exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        assertThat(result.failed()).isTrue();
        assertThat(result.cause()).isInstanceOf(PreBidException.class).hasMessage("Error");
    }

    @Test
    public void shouldNotModifyUserFromRequestIfNoBuyeridInCookie() {
        // given
        givenBidder(givenEmptySeatBid());

        // this is not required but stated for clarity's sake. The case when bidder is disabled.
        given(bidderCatalog.isActive(anyString())).willReturn(false);
        given(uidsCookie.uidFrom(any())).willReturn(null);

        final User user = User.builder().id("userId").build();
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.user(user));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(uidsCookie).uidFrom(isNull());

        final BidRequest capturedBidRequest = captureBidRequest();
        assertThat(capturedBidRequest.getUser()).isSameAs(user);
    }

    @Test
    public void shouldHonorBuyeridFromRequestAndClearBuyerIdsFromUserExtPrebidIfContains() {
        // given
        givenBidder(givenEmptySeatBid());

        given(uidsCookie.uidFrom(anyString())).willReturn("buyeridFromCookie");

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.user(User.builder()
                        .buyeruid("buyeridFromRequest")
                        .ext(ExtUser.builder()
                                .prebid(ExtUserPrebid.of(singletonMap("someBidder", "uidval")))
                                .build())
                        .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final User capturedBidRequestUser = captureBidRequest().getUser();
        assertThat(capturedBidRequestUser).isEqualTo(User.builder()
                .buyeruid("buyeridFromRequest")
                .build());
    }

    @Test
    public void shouldNotChangeGdprFromRequestWhenDeviceLmtIsOne() {
        // given
        givenBidder(givenEmptySeatBid());

        given(uidsCookie.uidFrom(anyString())).willReturn("buyeridFromCookie");

        final Regs regs = Regs.of(null, null);
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.user(User.builder().build())
                        .device(Device.builder().lmt(1).build())
                        .regs(regs));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final Regs capturedRegs = captureBidRequest().getRegs();
        assertThat(capturedRegs).isSameAs(regs);
    }

    @Test
    public void shouldDeepCopyImpExtContextToEachImpressionAndNotRemoveDataForAllWhenDeprecatedOnlyOneBidder() {
        // given
        final ObjectNode impExt = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .<ObjectNode>set("bidder", mapper.createObjectNode()
                                .put("someBidder", 1)
                                .put("deprecatedBidder", 2)))
                .set("context", mapper.createObjectNode()
                        .put("data", "data")
                        .put("otherField", "value"));
        final BidRequest bidRequest = givenBidRequest(singletonList(Imp.builder()
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())
                        .ext(impExt)
                        .build()),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                        .build())));
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(TEN).build())))));

        given(fpdResolver.resolveImpExt(any(), eq(true))).willReturn(mapper.createObjectNode()
                .set("context", mapper.createObjectNode()
                        .put("data", "data")
                        .put("otherField", "value")));
        given(fpdResolver.resolveImpExt(any(), eq(false))).willReturn(mapper.createObjectNode()
                .set("context", mapper.createObjectNode()
                        .put("otherField", "value")));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        assertThat(bidderRequestCaptor.getAllValues())
                .extracting(BidderRequest::getBidRequest)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .extracting(impExtNode -> impExtNode.get("context"))
                .containsOnly(
                        // data erased for deprecatedBidder
                        mapper.createObjectNode().put("otherField", "value"),
                        // data present for someBidder
                        mapper.createObjectNode().put("data", "data").put("otherField", "value"));
    }

    @Test
    public void shouldPassImpExtFieldsToEachImpression() {
        // given
        final ObjectNode impExt = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .<ObjectNode>set("bidder", mapper.createObjectNode()
                                .put("someBidder", 1)))
                .put("all", "allValue");

        final BidRequest bidRequest = givenBidRequest(
                singletonList(Imp.builder()
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build()).ext(impExt).build()),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                        .build())));
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(TEN).build())))));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        assertThat(bidderRequestCaptor.getAllValues())
                .extracting(BidderRequest::getBidRequest)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .extracting(impExtNode -> impExtNode.get("all"))
                .containsOnly(new TextNode("allValue"));
    }

    @Test
    public void shouldPassImpExtSkadnToEachImpression() {
        // given
        final ObjectNode impExt = mapper.createObjectNode()
                .<ObjectNode>set("prebid", mapper.createObjectNode()
                        .<ObjectNode>set("bidder", mapper.createObjectNode()
                                .put("someBidder", 1)))
                .put("skadn", "skadnValue");
        final BidRequest bidRequest = givenBidRequest(
                singletonList(Imp.builder()
                        .id("impId")
                        .banner(Banner.builder()
                                .format(singletonList(Format.builder().w(400).h(300).build()))
                                .build())
                        .ext(impExt)
                        .build()),
                identity());
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(TEN).build())))));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidRequestCaptor.capture(), any(), anyBoolean());
        assertThat(bidRequestCaptor.getAllValues())
                .extracting(BidderRequest::getBidRequest)
                .flatExtracting(BidRequest::getImp)
                .extracting(Imp::getExt)
                .extracting(impExtNode -> impExtNode.get("skadn"))
                .containsOnly(new TextNode("skadnValue"));
    }

    @Test
    public void shouldSetUserBuyerIdsFromUserExtPrebidAndClearPrebidBuyerIdsAfterwards() {
        // given
        givenBidder(givenEmptySeatBid());

        given(uidsCookie.uidFrom(anyString())).willReturn("buyeridFromCookie");

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder
                        .user(User.builder()
                                .ext(ExtUser.builder()
                                        .prebid(ExtUserPrebid.of(singletonMap("someBidder", "uidval")))
                                        .build())
                                .build())
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                                .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final User capturedBidRequestUser = captureBidRequest().getUser();
        assertThat(capturedBidRequestUser).isEqualTo(User.builder()
                .buyeruid("uidval")
                .build());
    }

    @Test
    public void shouldCleanRequestExtPrebidData() {
        // given
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .data(ExtRequestPrebidData.of(asList("someBidder", "should_be_removed"), null))
                        .aliases(singletonMap("someBidder", "alias_should_stay"))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ExtRequest capturedRequest = captureBidRequest().getExt();
        assertThat(capturedRequest).isEqualTo(ExtRequest.of(ExtRequestPrebid.builder()
                .aliases(singletonMap("someBidder", "alias_should_stay"))
                .auctiontimestamp(1000L)
                .build()));
    }

    @Test
    public void shouldAddMultiBidInfoAboutRequestedBidderIfDataShouldNotBeSuppressed() {
        // given
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .multibid(Collections.singletonList(
                                ExtRequestPrebidMultiBid.of("someBidder", null, 3, "prefix")))
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ExtRequest extRequest = captureBidRequest().getExt();
        assertThat(extRequest)
                .extracting(ExtRequest::getPrebid)
                .flatExtracting("multibid")
                .containsExactly(ExtRequestPrebidMultiBid.of("someBidder", null, 3, "prefix"));
    }

    @Test
    public void shouldAddMultibidInfoOnlyAboutRequestedBidder() {
        // given
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .multibid(Collections.singletonList(
                                ExtRequestPrebidMultiBid.of(null, asList("someBidder", "anotherBidder"), 3, null)))
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ExtRequest extRequest = captureBidRequest().getExt();
        assertThat(extRequest)
                .extracting(ExtRequest::getPrebid)
                .flatExtracting("multibid")
                .containsExactly(ExtRequestPrebidMultiBid.of("someBidder", null, 3, null));
    }

    @Test
    public void shouldPassUserDataAndExtDataOnlyForAllowedBidder() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);
        final List<ExtUserEid> eids = singletonList(ExtUserEid.of("eId", "id", emptyList(), null));
        final ExtUser extUser = ExtUser.builder().data(dataNode).eids(eids).build();
        final List<Data> data = singletonList(Data.builder().build());

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .auctiontimestamp(1000L)
                                .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                                .build()))
                        .user(User.builder()
                                .keywords("keyword")
                                .gender("male")
                                .yob(133)
                                .geo(Geo.EMPTY)
                                .ext(extUser)
                                .data(data)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        final ExtUser maskedExtUser = ExtUser.builder().eids(eids).build();
        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getKeywords, User::getGender, User::getYob, User::getGeo, User::getExt, User::getData)
                .containsOnly(
                        tuple("keyword", "male", 133, Geo.EMPTY, extUser, data),
                        tuple("keyword", "male", 133, Geo.EMPTY, maskedExtUser, null));
    }

    @Test
    public void shouldFilterUserExtEidsWhenBidderIsNotAllowedForSource() {
        testUserEidsPermissionFiltering(
                // given
                asList(
                        ExtUserEid.of("source1", null, null, null),
                        ExtUserEid.of("source2", null, null, null)),
                singletonList(ExtRequestPrebidDataEidPermissions.of("source1", singletonList("otherBidder"))),
                emptyMap(),
                // expected
                singletonList(ExtUserEid.of("source2", null, null, null))
        );
    }

    @Test
    public void shouldNotFilterUserExtEidsWhenEidsPermissionDoesNotContainSource() {
        testUserEidsPermissionFiltering(
                // given
                singletonList(ExtUserEid.of("source1", null, null, null)),
                singletonList(ExtRequestPrebidDataEidPermissions.of("source2", singletonList("otherBidder"))),
                emptyMap(),
                // expected
                singletonList(ExtUserEid.of("source1", null, null, null))
        );
    }

    @Test
    public void shouldNotFilterUserExtEidsWhenSourceAllowedForAllBidders() {
        testUserEidsPermissionFiltering(
                // given
                singletonList(ExtUserEid.of("source1", null, null, null)),
                singletonList(ExtRequestPrebidDataEidPermissions.of("source1", singletonList("*"))),
                emptyMap(),
                // expected
                singletonList(ExtUserEid.of("source1", null, null, null))
        );
    }

    @Test
    public void shouldNotFilterUserExtEidsWhenSourceAllowedForBidder() {
        testUserEidsPermissionFiltering(
                // given
                singletonList(ExtUserEid.of("source1", null, null, null)),
                singletonList(ExtRequestPrebidDataEidPermissions.of("source1", singletonList("someBidder"))),
                emptyMap(),
                // expected
                singletonList(ExtUserEid.of("source1", null, null, null))
        );
    }

    @Test
    public void shouldNotFilterUserExtEidsWhenSourceAllowedForBidderAlias() {
        testUserEidsPermissionFiltering(
                // given
                singletonList(ExtUserEid.of("source1", null, null, null)),
                singletonList(ExtRequestPrebidDataEidPermissions.of("source1", singletonList("someBidderAlias"))),
                singletonMap("someBidder", "someBidderAlias"),
                // expected
                singletonList(ExtUserEid.of("source1", null, null, null))
        );
    }

    @Test
    public void shouldFilterUserExtEidsWhenBidderIsNotAllowedForSourceAndSetNullIfNoEidsLeft() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        final Map<String, Integer> bidderToGdpr = singletonMap("someBidder", 1);
        final ExtUser extUser = ExtUser.builder().data(mapper.createObjectNode())
                .eids(singletonList(ExtUserEid.of("source1", null, null, null))).build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .data(ExtRequestPrebidData.of(null, singletonList(
                                        ExtRequestPrebidDataEidPermissions.of("source1",
                                                singletonList("otherBidder")))))
                                .build()))
                        .user(User.builder()
                                .ext(extUser)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();
        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getExt)
                .extracting(ExtUser::getEids)
                .element(0)
                .isNull();
    }

    @Test
    public void shouldNotCleanRequestExtPrebidDataWhenFpdAllowedAndPrebidIsNotNull() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = singletonMap("someBidder", 1);
        final ExtUser extUser = ExtUser.builder().prebid(ExtUserPrebid.of(emptyMap())).data(dataNode).build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .auctiontimestamp(1000L)
                                .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                                .build()))
                        .user(User.builder()
                                .ext(extUser)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();
        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getExt)
                .containsOnly(ExtUser.builder().data(dataNode).build());
    }

    @Test
    public void shouldMaskUserExtIfDataBiddersListIsEmpty() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);
        final List<ExtUserEid> eids = singletonList(ExtUserEid.of("eId", "id", emptyList(), null));
        final ExtUser extUser = ExtUser.builder().data(dataNode).eids(eids).build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .data(ExtRequestPrebidData.of(emptyList(), null)).build()))
                        .user(User.builder()
                                .keywords("keyword")
                                .gender("male")
                                .yob(133)
                                .geo(Geo.EMPTY)
                                .ext(extUser)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        final ExtUser expectedExtUser = ExtUser.builder().eids(eids).build();
        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getKeywords, User::getGender, User::getYob, User::getGeo, User::getExt)
                .containsOnly(
                        tuple("keyword", "male", 133, Geo.EMPTY, expectedExtUser),
                        tuple("keyword", "male", 133, Geo.EMPTY, expectedExtUser));
    }

    @Test
    public void shouldNoMaskUserExtIfDataBiddersListIsNull() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .data(ExtRequestPrebidData.of(null, null)).build()))
                        .user(User.builder()
                                .keywords("keyword")
                                .gender("male")
                                .yob(133)
                                .geo(Geo.EMPTY)
                                .ext(ExtUser.builder().data(dataNode).build())
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getKeywords, User::getGender, User::getYob, User::getGeo, User::getExt)
                .containsOnly(
                        tuple("keyword", "male", 133, Geo.EMPTY,
                                ExtUser.builder().data(dataNode).build()),
                        tuple("keyword", "male", 133, Geo.EMPTY,
                                ExtUser.builder().data(dataNode).build()));
    }

    @Test
    public void shouldPassSiteContentDataAndExtDataOnlyForAllowedBidder() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);
        final Content content = Content.builder()
                .data(singletonList(Data.builder().build()))
                .album("album")
                .build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .auctiontimestamp(1000L)
                        .data(ExtRequestPrebidData.of(singletonList("someBidder"), null)).build()))
                        .site(Site.builder()
                                .keywords("keyword")
                                .search("search")
                                .ext(ExtSite.of(0, dataNode))
                                .content(content)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getSite)
                .extracting(Site::getKeywords, Site::getSearch, Site::getExt, Site::getContent)
                .containsOnly(
                        tuple(
                                "keyword",
                                "search",
                                ExtSite.of(0, dataNode),
                                content),
                        tuple(
                                "keyword",
                                "search",
                                ExtSite.of(0, null),
                                Content.builder()
                                        .album("album")
                                        .build()));
    }

    @Test
    public void shouldNoMaskPassAppExtAndKeywordsWhenDataBiddersListIsNull() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .data(ExtRequestPrebidData.of(null, null)).build()))
                        .app(App.builder()
                                .keywords("keyword")
                                .ext(ExtApp.of(null, dataNode))
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getApp)
                .extracting(App::getExt, App::getKeywords)
                .containsOnly(
                        tuple(ExtApp.of(null, dataNode), "keyword"),
                        tuple(ExtApp.of(null, dataNode), "keyword"));
    }

    @Test
    public void shouldPassAppExtDataOnlyForAllowedBidder() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        givenBidder("missingBidder", bidder, givenEmptySeatBid());

        final ObjectNode dataNode = mapper.createObjectNode().put("data", "value");
        final Map<String, Integer> bidderToGdpr = doubleMap("someBidder", 1, "missingBidder", 0);
        final Content content = Content.builder()
                .data(singletonList(Data.builder().build()))
                .album("album")
                .build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .data(ExtRequestPrebidData.of(singletonList("someBidder"), null))
                                .auctiontimestamp(1000L)
                                .build()))
                        .app(App.builder()
                                .keywords("keyword")
                                .ext(ExtApp.of(null, dataNode))
                                .content(content)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester, times(2)).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getApp)
                .extracting(App::getExt, App::getKeywords, App::getContent)
                .containsOnly(
                        tuple(ExtApp.of(null, dataNode), "keyword", content),
                        tuple(null, "keyword", Content.builder().album("album").build()));
    }

    @Test
    public void shouldRejectRequestWhenAppAndSiteAppearsTogetherAfterFpdMerge() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        final ObjectNode bidderConfigApp = mapper.valueToTree(App.builder().id("appFromConfig").build());
        final ExtBidderConfig extBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(null, bidderConfigApp, null));
        final ExtRequestPrebidBidderConfig extRequestPrebidBidderConfig = ExtRequestPrebidBidderConfig.of(
                singletonList("someBidder"), extBidderConfig);
        final Site requestSite = Site.builder().id("erased").domain("domain").build();
        final ExtRequest extRequest = ExtRequest.of(
                ExtRequestPrebid.builder()
                        .bidderconfig(singletonList(extRequestPrebidBidderConfig))
                        .build());
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.site(requestSite).ext(extRequest));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verifyZeroInteractions(httpBidderRequester);
    }

    @Test
    public void shouldUseConcreteOverGeneralSiteWithExtPrebidBidderConfig() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());

        final ObjectNode siteWithPage = mapper.valueToTree(Site.builder().page("testPage").build());
        final ExtBidderConfig extBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(siteWithPage, null, null));
        final ExtRequestPrebidBidderConfig concreteFpdConfig = ExtRequestPrebidBidderConfig.of(
                singletonList("someBidder"), extBidderConfig);
        final ObjectNode siteWithDomain = mapper.valueToTree(Site.builder().domain("notUsed").build());
        final ExtBidderConfig allExtBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(siteWithDomain, null, null));
        final ExtRequestPrebidBidderConfig allFpdConfig = ExtRequestPrebidBidderConfig.of(singletonList("*"),
                allExtBidderConfig);

        final Site requestSite = Site.builder().id("siteId").page("erased").keywords("keyword").build();
        final ExtRequestPrebid extRequestPrebid = ExtRequestPrebid.builder()
                .bidderconfig(asList(allFpdConfig, concreteFpdConfig))
                .build();
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.site(requestSite).ext(ExtRequest.of(extRequestPrebid)));

        final Site mergedSite = Site.builder()
                .id("siteId")
                .page("testPage")
                .keywords("keyword")
                .build();

        given(fpdResolver.resolveSite(any(), any())).willReturn(mergedSite);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getSite)
                .containsOnly(mergedSite);
    }

    @Test
    public void shouldUseConcreteOverGeneralAppWithExtPrebidBidderConfig() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());

        final Publisher publisherWithId = Publisher.builder().id("testId").build();
        final ObjectNode appWithPublisherId = mapper.valueToTree(App.builder().publisher(publisherWithId).build());
        final ExtBidderConfig extBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(null, appWithPublisherId, null));
        final ExtRequestPrebidBidderConfig concreteFpdConfig = ExtRequestPrebidBidderConfig.of(
                singletonList("someBidder"), extBidderConfig);

        final Publisher publisherWithIdAndDomain = Publisher.builder().id("notUsed").domain("notUsed").build();
        final ObjectNode appWithUpdatedPublisher = mapper.valueToTree(
                App.builder().publisher(publisherWithIdAndDomain).build());
        final ExtBidderConfig allExtBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(null, appWithUpdatedPublisher, null));
        final ExtRequestPrebidBidderConfig allFpdConfig = ExtRequestPrebidBidderConfig.of(singletonList("*"),
                allExtBidderConfig);

        final App requestApp = App.builder().publisher(Publisher.builder().build()).build();

        final ExtRequestPrebid extRequestPrebid = ExtRequestPrebid.builder()
                .bidderconfig(asList(allFpdConfig, concreteFpdConfig))
                .build();
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.app(requestApp).ext(ExtRequest.of(extRequestPrebid)));
        final App mergedApp = App.builder()
                .publisher(Publisher.builder().id("testId").build())
                .build();

        given(fpdResolver.resolveApp(any(), any())).willReturn(mergedApp);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getApp)
                .containsOnly(mergedApp);
    }

    @Test
    public void shouldUseConcreteOverGeneralUserWithExtPrebidBidderConfig() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        final ObjectNode bidderConfigUser = mapper.valueToTree(User.builder().id("userFromConfig").build());
        final ExtBidderConfig extBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(null, null, bidderConfigUser));
        final ExtRequestPrebidBidderConfig concreteFpdConfig = ExtRequestPrebidBidderConfig.of(
                singletonList("someBidder"), extBidderConfig);

        final ObjectNode emptyUser = mapper.valueToTree(User.builder().build());
        final ExtBidderConfig allExtBidderConfig = ExtBidderConfig.of(
                null, ExtBidderConfigOrtb.of(null, null, emptyUser));
        final ExtRequestPrebidBidderConfig allFpdConfig = ExtRequestPrebidBidderConfig.of(singletonList("*"),
                allExtBidderConfig);
        final User requestUser = User.builder().id("erased").buyeruid("testBuyerId").build();

        final ExtRequestPrebid extRequestPrebid = ExtRequestPrebid.builder()
                .bidderconfig(asList(allFpdConfig, concreteFpdConfig))
                .build();
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.user(requestUser).ext(ExtRequest.of(extRequestPrebid)));

        final User mergedUser = User.builder().id("userFromConfig").buyeruid("testBuyerId").build();

        given(fpdResolver.resolveUser(any(), any())).willReturn(mergedUser);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();

        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .containsOnly(mergedUser);
    }

    @Test
    public void shouldAddBuyeridToUserFromRequest() {
        // given
        givenBidder(givenEmptySeatBid());
        given(uidsCookie.uidFrom(eq("cookieFamily"))).willReturn("buyerid");

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.user(User.builder().id("userId").build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final User capturedUser = captureBidRequest().getUser();
        assertThat(capturedUser).isEqualTo(User.builder().id("userId").buyeruid("buyerid").build());
    }

    @Test
    public void shouldCreateUserIfMissingInRequestAndBuyeridPresentInCookie() {
        // given
        givenBidder(givenEmptySeatBid());

        given(uidsCookie.uidFrom(eq("cookieFamily"))).willReturn("buyerid");

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final User capturedUser = captureBidRequest().getUser();
        assertThat(capturedUser).isEqualTo(User.builder().buyeruid("buyerid").build());
    }

    @Test
    public void shouldPassGlobalTimeoutToConnectorUnchangedIfCachingIsNotRequested() {
        // given
        givenBidder(givenEmptySeatBid());

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(httpBidderRequester).requestBids(any(), any(), same(timeout), anyBoolean());
    }

    @Test
    public void shouldPassReducedGlobalTimeoutToConnectorAndOriginalToBidResponseCreator() {
        // given
        exchangeService = new ExchangeService(
                100,
                bidderCatalog,
                storedResponseProcessor,
                privacyEnforcementService,
                fpdResolver,
                schainResolver,
                httpBidderRequester,
                responseBidValidator,
                currencyService,
                bidResponseCreator,
                bidResponsePostProcessor,
                metrics,
                clock,
                jacksonMapper);

        final Bid bid = Bid.builder().id("bidId1").impid("impId1").price(BigDecimal.valueOf(5.67)).build();
        givenBidder(givenSeatBid(singletonList(givenBid(bid))));

        final BidRequest bidRequest = givenBidRequest(singletonList(
                // imp ids are not really used for matching, included them here for clarity
                givenImp(singletonMap("bidder1", 1), builder -> builder.id("impId1"))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .targeting(givenTargeting(true))
                        .cache(ExtRequestPrebidCache.of(ExtRequestPrebidCacheBids.of(null, null), null, null))
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<Timeout> timeoutCaptor = ArgumentCaptor.forClass(Timeout.class);
        verify(httpBidderRequester).requestBids(any(), any(), timeoutCaptor.capture(), anyBoolean());
        assertThat(timeoutCaptor.getValue().remaining()).isEqualTo(400L);
        verify(bidResponseCreator).create(anyList(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void shouldReturnBidsWithUpdatedPriceCurrencyConversion() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2.0)).build()))));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                identity());

        final BigDecimal updatedPrice = BigDecimal.valueOf(5.0);
        given(currencyService.convertCurrency(any(), any(), any(), any())).willReturn(updatedPrice);

        givenBidResponseCreator(singletonList(Bid.builder().price(updatedPrice).build()));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid())
                .flatExtracting(SeatBid::getBid)
                .extracting(Bid::getPrice).containsExactly(updatedPrice);
    }

    @Test
    public void shouldReturnSameBidPriceIfNoChangesAppliedToBidPrice() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.ONE).build()))));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                identity());

        // returns the same price as in argument
        given(currencyService.convertCurrency(any(), any(), any(), any()))
                .willAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid())
                .flatExtracting(SeatBid::getBid)
                .extracting(Bid::getPrice).containsExactly(BigDecimal.ONE);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldDropBidIfPrebidExceptionWasThrownDuringCurrencyConversion() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2.0)).build(), "CUR"))));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                identity());

        given(currencyService.convertCurrency(any(), any(), any(), any()))
                .willThrow(new PreBidException("Unable to convert bid currency CUR to desired ad server currency USD"));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());

        assertThat(argumentCaptor.getValue()).hasSize(1);

        final BidderError expectedError =
                BidderError.generic("Unable to convert bid currency CUR to desired ad server currency USD");
        final BidderSeatBid firstSeatBid = argumentCaptor.getValue().get(0).getSeatBid();
        assertThat(firstSeatBid.getBids()).isEmpty();
        assertThat(firstSeatBid.getErrors()).containsOnly(expectedError);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateBidPriceWithCurrencyConversionAndPriceAdjustmentFactor() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2.0)).build()))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder().build();
        givenAdjustments.addFactor("bidder", BigDecimal.valueOf(10));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        given(currencyService.convertCurrency(any(), any(), any(), any()))
                .willReturn(BigDecimal.valueOf(10));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());

        assertThat(argumentCaptor.getValue()).hasSize(1);

        final BigDecimal updatedPrice = BigDecimal.valueOf(100);
        final BidderSeatBid firstSeatBid = argumentCaptor.getValue().get(0).getSeatBid();
        assertThat(firstSeatBid.getBids())
                .extracting(BidderBid::getBid)
                .flatExtracting(Bid::getPrice)
                .containsOnly(updatedPrice);
        assertThat(firstSeatBid.getErrors()).isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdatePriceForOneBidAndDropAnotherIfPrebidExceptionHappensForSecondBid() {
        // given
        final BigDecimal firstBidderPrice = BigDecimal.valueOf(2.0);
        final BigDecimal secondBidderPrice = BigDecimal.valueOf(3.0);
        givenBidder("bidder", mock(Bidder.class), givenSeatBid(asList(
                givenBid(Bid.builder().price(firstBidderPrice).build(), "CUR1"),
                givenBid(Bid.builder().price(secondBidderPrice).build(), "CUR2"))));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                identity());

        final BigDecimal updatedPrice = BigDecimal.valueOf(10.0);
        given(currencyService.convertCurrency(any(), any(), any(), any())).willReturn(updatedPrice)
                .willThrow(
                        new PreBidException("Unable to convert bid currency CUR2 to desired ad server currency USD"));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());
        verify(currencyService).convertCurrency(eq(firstBidderPrice), eq(bidRequest), any(), eq("CUR1"));
        verify(currencyService).convertCurrency(eq(secondBidderPrice), eq(bidRequest), any(), eq("CUR2"));

        assertThat(argumentCaptor.getValue()).hasSize(1);

        final ObjectNode expectedBidExt = mapper.createObjectNode();
        expectedBidExt.put("origbidcpm", new BigDecimal("2.0"));
        expectedBidExt.put("origbidcur", "CUR1");
        final Bid expectedBid = Bid.builder().price(updatedPrice).ext(expectedBidExt).build();

        final BidderBid expectedBidderBid = BidderBid.of(expectedBid, banner, "CUR1");
        final BidderError expectedError =
                BidderError.generic("Unable to convert bid currency CUR2 to desired ad server currency USD");

        final BidderSeatBid firstSeatBid = argumentCaptor.getValue().get(0).getSeatBid();
        assertThat(firstSeatBid.getBids()).containsOnly(expectedBidderBid);
        assertThat(firstSeatBid.getErrors()).containsOnly(expectedError);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldRespondWithOneBidAndErrorWhenBidResponseContainsOneUnsupportedCurrency() {
        // given
        final BigDecimal firstBidderPrice = BigDecimal.valueOf(2.0);
        final BigDecimal secondBidderPrice = BigDecimal.valueOf(10.0);
        givenBidder("bidder1", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(firstBidderPrice).build(), "USD"))));
        givenBidder("bidder2", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(10.0)).build(), "CUR"))));

        final BidRequest bidRequest = BidRequest.builder().cur(singletonList("BAD"))
                .imp(singletonList(givenImp(doubleMap("bidder1", 2, "bidder2", 3),
                        identity()))).build();

        final BigDecimal updatedPrice = BigDecimal.valueOf(20);
        given(currencyService.convertCurrency(any(), any(), any(), any())).willReturn(updatedPrice);
        given(currencyService.convertCurrency(any(), any(), eq("BAD"), eq("CUR")))
                .willThrow(new PreBidException("Unable to convert bid currency CUR to desired ad server currency BAD"));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());
        verify(currencyService).convertCurrency(eq(firstBidderPrice), eq(bidRequest), eq("BAD"), eq("USD"));
        verify(currencyService).convertCurrency(eq(secondBidderPrice), eq(bidRequest), eq("BAD"), eq("CUR"));

        assertThat(argumentCaptor.getValue()).hasSize(2);

        final ObjectNode expectedBidExt = mapper.createObjectNode();
        expectedBidExt.put("origbidcpm", new BigDecimal("2.0"));
        expectedBidExt.put("origbidcur", "USD");
        final Bid expectedBid = Bid.builder().price(updatedPrice).ext(expectedBidExt).build();
        final BidderBid expectedBidderBid = BidderBid.of(expectedBid, banner, "USD");
        assertThat(argumentCaptor.getValue())
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .containsOnly(expectedBidderBid);

        final BidderError expectedError =
                BidderError.generic("Unable to convert bid currency CUR to desired ad server currency BAD");
        assertThat(argumentCaptor.getValue())
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getErrors)
                .containsOnly(expectedError);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateBidPriceWithCurrencyConversionAndAddErrorAboutMultipleCurrency() {
        // given
        final BigDecimal bidderPrice = BigDecimal.valueOf(2.0);
        givenBidder("bidder", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(bidderPrice).build(), "USD"))));

        final BidRequest bidRequest = givenBidRequest(
                singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.cur(asList("CUR1", "CUR2", "CUR2")));

        final BigDecimal updatedPrice = BigDecimal.valueOf(10.0);
        given(currencyService.convertCurrency(any(), any(), any(), any())).willReturn(updatedPrice);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());
        verify(currencyService).convertCurrency(eq(bidderPrice), eq(bidRequest), eq("CUR1"), eq("USD"));

        assertThat(argumentCaptor.getValue()).hasSize(1);

        final BidderError expectedError = BidderError.badInput("Cur parameter contains more than one currency."
                + " CUR1 will be used");
        final BidderSeatBid firstSeatBid = argumentCaptor.getValue().get(0).getSeatBid();
        assertThat(firstSeatBid.getBids())
                .extracting(BidderBid::getBid)
                .flatExtracting(Bid::getPrice)
                .containsOnly(updatedPrice);
        assertThat(firstSeatBid.getErrors()).containsOnly(expectedError);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldUpdateBidPriceWithCurrencyConversionForMultipleBid() {
        // given
        final BigDecimal bidder1Price = BigDecimal.valueOf(1.5);
        final BigDecimal bidder2Price = BigDecimal.valueOf(2);
        final BigDecimal bidder3Price = BigDecimal.valueOf(3);
        givenBidder("bidder1", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(bidder1Price).build(), "EUR"))));
        givenBidder("bidder2", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(bidder2Price).build(), "GBP"))));
        givenBidder("bidder3", mock(Bidder.class), givenSeatBid(singletonList(
                givenBid(Bid.builder().price(bidder3Price).build(), "USD"))));

        final Map<String, Integer> impBidders = new HashMap<>();
        impBidders.put("bidder1", 1);
        impBidders.put("bidder2", 2);
        impBidders.put("bidder3", 3);
        final BidRequest bidRequest = givenBidRequest(
                singletonList(givenImp(impBidders, identity())), builder -> builder.cur(singletonList("USD")));

        final BigDecimal updatedPrice = BigDecimal.valueOf(10.0);
        given(currencyService.convertCurrency(any(), any(), any(), any())).willReturn(updatedPrice);
        given(currencyService.convertCurrency(any(), any(), any(), eq("USD"))).willReturn(bidder3Price);

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final ArgumentCaptor<List<BidderResponse>> argumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(argumentCaptor.capture(), any(), any(), any(), anyBoolean());
        verify(currencyService).convertCurrency(eq(bidder1Price), eq(bidRequest), eq("USD"), eq("EUR"));
        verify(currencyService).convertCurrency(eq(bidder2Price), eq(bidRequest), eq("USD"), eq("GBP"));
        verify(currencyService).convertCurrency(eq(bidder3Price), eq(bidRequest), eq("USD"), eq("USD"));
        verifyNoMoreInteractions(currencyService);

        assertThat(argumentCaptor.getValue())
                .hasSize(3)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsOnly(bidder3Price, updatedPrice, updatedPrice);
    }

    @Test
    public void shouldNotAddExtPrebidEventsWhenEventsServiceReturnsEmptyEventsService() {
        // given
        final BigDecimal price = BigDecimal.valueOf(2.0);
        givenBidder(BidderSeatBid.of(
                singletonList(BidderBid.of(
                        Bid.builder().id("bidId").price(price)
                                .ext(mapper.valueToTree(singletonMap("bidExt", 1))).build(), banner, null)),
                emptyList(),
                emptyList()));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                bidRequestBuilder -> bidRequestBuilder.app(App.builder()
                        .publisher(Publisher.builder().id("1001").build()).build()));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid()).hasSize(1)
                .flatExtracting(SeatBid::getBid)
                .extracting(bid -> toExtPrebid(bid.getExt()).getPrebid().getEvents())
                .containsNull();
    }

    @Test
    public void shouldIncrementCommonMetrics() {
        // given
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(TEN).build())))));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someAlias", 1)),
                builder -> builder
                        .site(Site.builder().publisher(Publisher.builder().id("accountId").build()).build())
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .aliases(singletonMap("someAlias", "someBidder"))
                                .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(metrics).updateRequestBidderCardinalityMetric(1);
        verify(metrics).updateAccountRequestMetrics(eq("accountId"), eq(MetricName.openrtb2web));
        verify(metrics)
                .updateAdapterRequestTypeAndNoCookieMetrics(eq("someBidder"), eq(MetricName.openrtb2web), eq(true));
        verify(metrics).updateAdapterResponseTime(eq("someBidder"), eq("accountId"), anyInt());
        verify(metrics).updateAdapterRequestGotbidsMetrics(eq("someBidder"), eq("accountId"));
        verify(metrics).updateAdapterBidMetrics(eq("someBidder"), eq("accountId"), eq(10000L), eq(false), eq("banner"));
    }

    @Test
    public void shouldCallUpdateCookieMetricsWithExpectedValue() {
        // given
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)),
                builder -> builder.app(App.builder().build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(metrics).updateAdapterRequestTypeAndNoCookieMetrics(
                eq("someBidder"), eq(MetricName.openrtb2web), eq(false));
    }

    @Test
    public void shouldUseEmptyStringIfPublisherIdIsEmpty() {
        // given
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(singletonList(
                        givenBid(Bid.builder().price(TEN).build())))));
        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));
        final Account account = Account.builder().id("").build();

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest, account));

        // then
        verify(metrics).updateAccountRequestMetrics(eq(""), eq(MetricName.openrtb2web));
    }

    @Test
    public void shouldIncrementNoBidRequestsMetric() {
        // given
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenSeatBid(emptyList())));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(metrics).updateAdapterRequestNobidMetrics(eq("someBidder"), eq("accountId"));
    }

    @Test
    public void shouldIncrementGotBidsAndErrorMetricsIfBidderReturnsBidAndDifferentErrors() {
        // given
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(BidderSeatBid.of(
                        singletonList(givenBid(Bid.builder().price(TEN).build())),
                        emptyList(),
                        asList(
                                // two identical errors to verify corresponding metric is submitted only once
                                BidderError.badInput("rubicon error"),
                                BidderError.badInput("rubicon error"),
                                BidderError.badServerResponse("rubicon error"),
                                BidderError.failedToRequestBids("rubicon failed to request bids"),
                                BidderError.timeout("timeout error"),
                                BidderError.generic("timeout error")))));

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(singletonMap("someBidder", 1)));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(metrics).updateAdapterRequestGotbidsMetrics(eq("someBidder"), eq("accountId"));
        verify(metrics).updateAdapterRequestErrorMetric(eq("someBidder"), eq(MetricName.badinput));
        verify(metrics).updateAdapterRequestErrorMetric(eq("someBidder"), eq(MetricName.badserverresponse));
        verify(metrics).updateAdapterRequestErrorMetric(eq("someBidder"), eq(MetricName.failedtorequestbids));
        verify(metrics).updateAdapterRequestErrorMetric(eq("someBidder"), eq(MetricName.timeout));
        verify(metrics).updateAdapterRequestErrorMetric(eq("someBidder"), eq(MetricName.unknown_error));
    }

    @Test
    public void shouldPassResponseToPostProcessor() {
        // given
        final BidRequest bidRequest = givenBidRequest(emptyList());

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        verify(bidResponsePostProcessor).postProcess(any(), same(uidsCookie), same(bidRequest), any(),
                eq(Account.builder().id("accountId").eventsEnabled(true).build()));
    }

    @Test
    public void shouldReturnBidsWithAdjustedPricesWhenAdjustmentFactorPresent() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2)).build()))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder().build();
        givenAdjustments.addFactor("bidder", BigDecimal.valueOf(2.468));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final List<BidderResponse> capturedBidResponses = captureBidResponses();
        assertThat(capturedBidResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.valueOf(4.936));
    }

    @Test
    public void shouldReturnBidsWithAdjustedPricesWithVideoInstreamMediaTypeIfVideoPlacementEqualsOne() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                BidderBid.of(Bid.builder().impid("123").price(BigDecimal.valueOf(2)).build(), video, null))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder()
                .mediatypes(new EnumMap<>(Collections.singletonMap(BidAdjustmentMediaType.video,
                        Collections.singletonMap("bidder", BigDecimal.valueOf(3.456)))))
                .build();

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), impBuilder ->
                        impBuilder.id("123").video(Video.builder().placement(1).build()))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final List<BidderResponse> capturedBidResponses = captureBidResponses();
        assertThat(capturedBidResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.valueOf(6.912));
    }

    @Test
    public void shouldReturnBidsWithAdjustedPricesWithVideoInstreamMediaTypeIfVideoPlacementIsMissing() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                BidderBid.of(Bid.builder().impid("123").price(BigDecimal.valueOf(2)).build(), video, null))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder()
                .mediatypes(new EnumMap<>(Collections.singletonMap(BidAdjustmentMediaType.video,
                        Collections.singletonMap("bidder", BigDecimal.valueOf(3.456)))))
                .build();

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), impBuilder ->
                        impBuilder.id("123").video(Video.builder().build()))),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final List<BidderResponse> capturedBidResponses = captureBidResponses();
        assertThat(capturedBidResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.valueOf(6.912));
    }

    @Test
    public void shouldReturnBidsWithAdjustedPricesWhenAdjustmentMediaFactorPresent() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2)).build()))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder()
                .mediatypes(new EnumMap<>(Collections.singletonMap(BidAdjustmentMediaType.banner,
                        Collections.singletonMap("bidder", BigDecimal.valueOf(3.456)))))
                .build();

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final List<BidderResponse> capturedBidResponses = captureBidResponses();
        assertThat(capturedBidResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.valueOf(6.912));
    }

    @Test
    public void shouldAdjustPriceWithPriorityForMediaTypeAdjustment() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.valueOf(2)).build()))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder()
                .mediatypes(new EnumMap<>(Collections.singletonMap(BidAdjustmentMediaType.banner,
                        Collections.singletonMap("bidder", BigDecimal.valueOf(3.456)))))
                .build();
        givenAdjustments.addFactor("bidder", BigDecimal.valueOf(2.468));

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .bidadjustmentfactors(givenAdjustments)
                        .auctiontimestamp(1000L)
                        .build())));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        final List<BidderResponse> capturedBidResponses = captureBidResponses();
        assertThat(capturedBidResponses)
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.valueOf(6.912));
    }

    @Test
    public void shouldReturnBidsWithoutAdjustingPricesWhenAdjustmentFactorNotPresentForBidder() {
        // given
        final Bidder<?> bidder = mock(Bidder.class);

        givenBidder("bidder", bidder, givenSeatBid(singletonList(
                givenBid(Bid.builder().price(BigDecimal.ONE).build()))));

        final ExtRequestBidadjustmentfactors givenAdjustments = ExtRequestBidadjustmentfactors.builder().build();
        givenAdjustments.addFactor("some-other-bidder", BigDecimal.TEN);

        final BidRequest bidRequest = givenBidRequest(singletonList(givenImp(singletonMap("bidder", 2), identity())),
                builder -> builder.ext(ExtRequest.of(ExtRequestPrebid.builder()
                        .aliases(emptyMap())
                        .auctiontimestamp(1000L)
                        .currency(ExtRequestCurrency.of(null, false))
                        .bidadjustmentfactors(givenAdjustments)
                        .build())));

        // when
        final BidResponse bidResponse = exchangeService.holdAuction(givenRequestContext(bidRequest)).result();

        // then
        assertThat(bidResponse.getSeatbid())
                .flatExtracting(SeatBid::getBid)
                .extracting(Bid::getPrice)
                .containsExactly(BigDecimal.ONE);
    }

    private AuctionContext givenRequestContext(BidRequest bidRequest) {
        return givenRequestContext(bidRequest, Account.builder().id("accountId").eventsEnabled(true).build());
    }

    private AuctionContext givenRequestContext(BidRequest bidRequest, Account account) {
        return AuctionContext.builder()
                .uidsCookie(uidsCookie)
                .bidRequest(bidRequest)
                .debugWarnings(new ArrayList<>())
                .account(account)
                .requestTypeMetric(MetricName.openrtb2web)
                .timeout(timeout)
                .build();
    }

    private BidRequest captureBidRequest() {
        final ArgumentCaptor<BidderRequest> bidRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidRequestCaptor.capture(), any(), anyBoolean());
        return bidRequestCaptor.getValue().getBidRequest();
    }

    private List<BidderResponse> captureBidResponses() {
        final ArgumentCaptor<List<BidderResponse>> bidderResponseCaptor = ArgumentCaptor.forClass(List.class);
        verify(bidResponseCreator).create(bidderResponseCaptor.capture(), any(), any(), any(), anyBoolean());
        return bidderResponseCaptor.getValue();
    }

    private static BidRequest givenBidRequest(
            List<Imp> imp, Function<BidRequestBuilder, BidRequestBuilder> bidRequestBuilderCustomizer) {
        return bidRequestBuilderCustomizer.apply(BidRequest.builder().cur(singletonList("USD")).imp(imp)).build();
    }

    private static BidRequest givenBidRequest(List<Imp> imp) {
        return givenBidRequest(imp, identity());
    }

    private static <T> Imp givenImp(T ext, Function<ImpBuilder, ImpBuilder> impBuilderCustomizer) {
        return impBuilderCustomizer.apply(Imp.builder()
                .ext(mapper.valueToTree(singletonMap(
                        "prebid", ext != null ? singletonMap("bidder", ext) : emptyMap()))))
                .build();
    }

    private static <T> List<Imp> givenSingleImp(T ext) {
        return singletonList(givenImp(ext, identity()));
    }

    private void givenBidder(BidderSeatBid response) {
        given(httpBidderRequester.requestBids(any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(response));
    }

    private void givenBidder(String bidderName, Bidder<?> bidder, BidderSeatBid response) {
        doReturn(bidder).when(bidderCatalog).bidderByName(eq(bidderName));
        given(httpBidderRequester.requestBids(same(bidder), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(response));
    }

    private static SeatBid givenSeatBid(List<Bid> bids,
                                        Function<SeatBid.SeatBidBuilder, SeatBid.SeatBidBuilder> seatBidCustomizer) {
        return seatBidCustomizer.apply(SeatBid.builder()
                .seat("someBidder")
                .bid(bids))
                .build();
    }

    private static BidderSeatBid givenSeatBid(List<BidderBid> bids) {
        return BidderSeatBid.of(bids, emptyList(), emptyList());
    }

    private static BidderSeatBid givenSingleSeatBid(BidderBid bid) {
        return givenSeatBid(singletonList(bid));
    }

    private static BidderSeatBid givenEmptySeatBid() {
        return givenSeatBid(emptyList());
    }

    private static BidderBid givenBid(Bid bid) {
        return BidderBid.of(bid, BidType.banner, null);
    }

    private static BidderBid givenBid(Bid bid, String cur) {
        return BidderBid.of(bid, BidType.banner, cur);
    }

    private static Bid givenBid(Function<Bid.BidBuilder, Bid.BidBuilder> bidBuilder) {
        return bidBuilder.apply(Bid.builder()
                .id("bidId")
                .price(BigDecimal.ONE)
                .ext(mapper.valueToTree(ExtPrebid.of(ExtBidPrebid.builder().build(), null))))
                .build();
    }

    private static <K, V> Map<K, V> doubleMap(K key1, V value1, K key2, V value2) {
        final Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    private static ExtPrebid<ExtBidPrebid, ?> toExtPrebid(ObjectNode ext) {
        try {
            return mapper.readValue(mapper.treeAsTokens(ext), new TypeReference<ExtPrebid<ExtBidPrebid, ?>>() {
            });
        } catch (IOException e) {
            return rethrow(e);
        }
    }

    private static ExtRequestTargeting givenTargeting(boolean includebidderkeys) {
        return ExtRequestTargeting.builder().pricegranularity(mapper.valueToTree(
                ExtPriceGranularity.of(2, singletonList(ExtGranularityRange.of(BigDecimal.valueOf(5),
                        BigDecimal.valueOf(0.5))))))
                .includewinners(true)
                .includebidderkeys(includebidderkeys)
                .build();
    }

    private void givenBidResponseCreator(List<Bid> bids) {
        given(bidResponseCreator.create(anyList(), any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenBidResponseWithBids(bids)));
    }

    private void givenBidResponseCreator(Map<String, List<ExtBidderError>> errors) {
        given(bidResponseCreator.create(anyList(), any(), any(), any(), anyBoolean()))
                .willReturn(Future.succeededFuture(givenBidResponseWithError(errors)));
    }

    private static BidResponse givenBidResponseWithBids(List<Bid> bids) {
        return BidResponse.builder()
                .cur("USD")
                .seatbid(singletonList(givenSeatBid(bids, identity())))
                .build();
    }

    private static BidResponse givenBidResponseWithError(Map<String, List<ExtBidderError>> errors) {
        return BidResponse.builder()
                .seatbid(emptyList())
                .ext(mapper.valueToTree(ExtBidResponse.of(null, errors, null, null, null, null, null)))
                .build();
    }

    private void testUserEidsPermissionFiltering(List<ExtUserEid> givenExtUserEids,
                                                 List<ExtRequestPrebidDataEidPermissions> givenEidPermissions,
                                                 Map<String, String> givenAlises,
                                                 List<ExtUserEid> expectedExtUserEids) {
        // given
        final Bidder<?> bidder = mock(Bidder.class);
        givenBidder("someBidder", bidder, givenEmptySeatBid());
        final Map<String, Integer> bidderToGdpr = singletonMap("someBidder", 1);
        final ExtUser extUser = ExtUser.builder().eids(givenExtUserEids).build();

        final BidRequest bidRequest = givenBidRequest(givenSingleImp(bidderToGdpr),
                builder -> builder
                        .ext(ExtRequest.of(ExtRequestPrebid.builder()
                                .aliases(givenAlises)
                                .data(ExtRequestPrebidData.of(null, givenEidPermissions))
                                .build()))
                        .user(User.builder()
                                .ext(extUser)
                                .build()));

        // when
        exchangeService.holdAuction(givenRequestContext(bidRequest));

        // then
        final ArgumentCaptor<BidderRequest> bidderRequestCaptor = ArgumentCaptor.forClass(BidderRequest.class);
        verify(httpBidderRequester).requestBids(any(), bidderRequestCaptor.capture(), any(), anyBoolean());
        final List<BidderRequest> capturedBidRequests = bidderRequestCaptor.getAllValues();
        assertThat(capturedBidRequests)
                .extracting(BidderRequest::getBidRequest)
                .extracting(BidRequest::getUser)
                .extracting(User::getExt)
                .flatExtracting(ExtUser::getEids)
                .isEqualTo(expectedExtUserEids);
    }
}
