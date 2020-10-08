package org.prebid.server.auction;

import com.iab.openrtb.response.Bid;
import io.vertx.core.Future;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.prebid.server.VertxTest;
import org.prebid.server.auction.model.BidderResponse;
import org.prebid.server.auction.model.CategoryMappingResult;
import org.prebid.server.bidder.model.BidderBid;
import org.prebid.server.bidder.model.BidderSeatBid;
import org.prebid.server.exception.InvalidRequestException;
import org.prebid.server.proto.openrtb.ext.ExtIncludeBrandCategory;
import org.prebid.server.proto.openrtb.ext.request.ExtMediaTypePriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtPriceGranularity;
import org.prebid.server.proto.openrtb.ext.request.ExtRequestTargeting;
import org.prebid.server.proto.openrtb.ext.response.BidType;
import org.prebid.server.proto.openrtb.ext.response.ExtBidPrebidVideo;
import org.prebid.server.settings.ApplicationSettings;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class CategoryMapperTest extends VertxTest {

    @Rule
    public final MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    ApplicationSettings applicationSettings;

    private CategoryMapper categoryMapper;

    private static PriceGranularity priceGranularity;

    @Before
    public void setUp() {
        categoryMapper = new CategoryMapper(applicationSettings, jacksonMapper);
        priceGranularity = PriceGranularity.DEFAULT;
    }

    @Test
    public void applyCategoryMappingShouldReturnFilteredBidsWithCategory() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1"),
                        givenBidderBid("2", "15", BidType.video, singletonList("cat2"), 15, "prCategory2")),
                givenBidderResponse("otherBid", givenBidderBid("3", "10", BidType.video, singletonList("cat3"), 3,
                        "prCategory3"),
                        givenBidderBid("4", "15", BidType.video, singletonList("cat4"), 1, "prCategory4")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        // first and third fetch will have conflict, so one bid should be filtered in result
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"),
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat4"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        final Map<String, Map<String, String>> expectedBidCategory = new HashMap<>();
        final Map<String, String> rubiconBidToCategory = new HashMap<>();
        rubiconBidToCategory.put("1", "10.00_fetchedCat1_10s");
        rubiconBidToCategory.put("2", "15.00_fetchedCat2_15s");
        expectedBidCategory.put("rubicon", rubiconBidToCategory);
        expectedBidCategory.put("otherBid", Collections.singletonMap("4", "15.00_fetchedCat4_5s"));
        assertThat(resultFuture.result().getBidderToBidCategory()).isEqualTo(expectedBidCategory);
        assertThat(resultFuture.result().getBidderResponses())
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid).hasSize(3)
                .extracting(Bid::getId)
                .containsOnly("1", "2", "4");
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 3] with a reason: Bid was deduplicated");
    }

    @Test
    public void applyCategoryMappingShouldTolerateBidsWithSameIdWithingDifferentBidders() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("1", "5", BidType.video, singletonList("cat2"), 3,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        final Map<String, Map<String, String>> expectedBidCategory = new HashMap<>();
        expectedBidCategory.put("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s"));
        expectedBidCategory.put("otherBid", Collections.singletonMap("1", "5.00_fetchedCat2_5s"));
        assertThat(resultFuture.result().getBidderToBidCategory()).isEqualTo(expectedBidCategory);
        assertThat(resultFuture.result().getBidderResponses())
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid).hasSize(2)
                .extracting(Bid::getId)
                .containsOnly("1", "1");
        assertThat(resultFuture.result().getErrors()).isEmpty();
    }

    @Test
    public void applyCategoryMappingShouldNotCallFetchCategoryWhenTranslateCategoriesFalse() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, false);

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        verifyZeroInteractions(applicationSettings);
        assertThat(resultFuture.succeeded()).isTrue();
        final Map<String, Map<String, String>> expectedBidCategory = new HashMap<>();
        expectedBidCategory.put("rubicon", Collections.singletonMap("1", "10.00_cat1_10s"));
        assertThat(resultFuture.result().getBidderToBidCategory()).isEqualTo(expectedBidCategory);
    }

    @Test
    public void applyCategoryMappingShouldReturnFailedFutureWhenTranslateTrueAndAdServerNull() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(null, "publisher", null, true, true);

        // when
        final Future<CategoryMappingResult> categoryMappingResultFuture =
                categoryMapper.applyCategoryMapping(bidderResponses, extRequestTargeting);

        // then
        assertThat(categoryMappingResultFuture.failed()).isTrue();
        assertThat(categoryMappingResultFuture.cause())
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("Primary ad server required but was not defined when translate category is enabled");
    }

    @Test
    public void applyCategoryMappingShouldReturnFailedFutureWhenTranslateTrueAndAdServerIsThree() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(3, "publisher", null, true, true);

        // when
        final Future<CategoryMappingResult> categoryMappingResultFuture =
                categoryMapper.applyCategoryMapping(bidderResponses, extRequestTargeting);

        // then
        assertThat(categoryMappingResultFuture.failed()).isTrue();
        assertThat(categoryMappingResultFuture.cause())
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage("Primary ad server `3` is not recognized");
    }

    @Test
    public void applyCategoryMappingShouldReturnUseFreewheelAdServerWhenAdServerIs1() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);

        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        categoryMapper.applyCategoryMapping(bidderResponses, extRequestTargeting);

        // then
        verify(applicationSettings).getCategory(eq("freewheel"), anyString(), anyString());
    }

    @Test
    public void applyCategoryMappingShouldReturnUseDpfAdServerWhenAdServerIs2() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(2, "publisher",
                asList(10, 15, 5), true, true);

        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        categoryMapper.applyCategoryMapping(bidderResponses, extRequestTargeting);

        // then
        verify(applicationSettings).getCategory(eq("dfp"), anyString(), anyString());
    }

    @Test
    public void applyCategoryMappingShouldRejectBidsWithFailedCategoryFetch() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, singletonList("cat2"), 3,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), eq("cat1"))).willReturn(
                Future.succeededFuture("fetchedCat1"));
        given(applicationSettings.getCategory(anyString(), anyString(), eq("cat2"))).willReturn(
                Future.failedFuture(new TimeoutException("Timeout")));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Timeout");
    }

    @Test
    public void applyCategoryMappingShouldRejectBidsWithCatLengthMoreThanOne() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, asList("cat2-1", "cat2-2"), 3,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Bid has more than one"
                        + " category");
    }

    @Test
    public void applyCategoryMappingShouldRejectBidsWithWhenCatIsNull() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, null, 3,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Bid did not contain a"
                        + " category");
    }

    @Test
    public void applyCategoryMappingShouldRejectBidWhenNullCategoryReturnedFromSource() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, singletonList("cat2"), 3,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture(null));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Category mapping storage"
                        + " for primary ad server: 'freewheel', publisher: 'publisher' not found");
    }

    @Test
    public void applyCategoryMappingShouldUseMediaTypePriceGranularityIfDefined() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = ExtRequestTargeting.builder()
                .pricegranularity(mapper.valueToTree(ExtPriceGranularity.from(priceGranularity)))
                .mediatypepricegranularity(ExtMediaTypePriceGranularity.of(null,
                        mapper.valueToTree(ExtPriceGranularity.from(PriceGranularity.createFromString("low"))), null))
                .includebrandcategory(ExtIncludeBrandCategory.of(1, "publisher", true, true))
                .durationrangesec(asList(10, 15, 5))
                .build();

        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "5.00_fetchedCat1_10s")));
    }

    @Test
    public void applyCategoryMappingShouldRejectBidIfItsDurationLargerThanTargetingMax() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, singletonList("cat2"), 20,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Bid duration '20' "
                        + "exceeds maximum '15'");
    }

    @Test
    public void applyCategoryMappingShouldSetFirstDurationFromRangeIfDurationIsNull() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), null,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_5s")));
    }

    @Test
    public void applyCategoryMappingShouldDeduplicateBidsByFetchedCategoryWhenWithCategoryIsTrue() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, singletonList("cat2"), 4,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat1"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Bid was deduplicated");
    }

    @Test
    public void applyCategoryMappingShouldDeduplicateBidsByBidCatWhenWithCategoryIsTrueAndTranslateFalse() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "5", BidType.video, singletonList("cat1"), 4,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), true, false);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_cat1_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: otherBid, bid ID: 2] with a reason: Bid was deduplicated");
    }

    @Test
    public void applyCategoryMappingShouldDeduplicateBidsByPriceAndDurationIfWithCategoryFalse() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")),
                givenBidderResponse("otherBid", givenBidderBid("2", "10", BidType.video, singletonList("cat2"), 10,
                        "prCategory2")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher",
                asList(10, 15, 5), false, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"));
        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("otherBid", Collections.singletonMap("2", "10.00_10s")));
        assertThat(resultFuture.result().getErrors()).hasSize(1)
                .containsOnly("Bid rejected [bidder: rubicon, bid ID: 1] with a reason: Bid was deduplicated");
    }

    @Test
    public void applyCategoryMappingShouldReturnDurCatBuiltFromPriceAndFetchedCategoryAndDuration() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher", asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
    }

    @Test
    public void applyCategoryMappingShouldReturnDurCatBuiltFromPriceAndBidCatAndDuration() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher", asList(10, 15, 5), true, false);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_cat1_10s")));
    }

    @Test
    public void applyCategoryMappingShouldReturnDurCatBuiltFromPriceAndDuration() {
        // given
        final List<BidderResponse> bidderResponses = singletonList(
                givenBidderResponse("rubicon", givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10,
                        "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher", asList(10, 15, 5), false, null);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon", Collections.singletonMap("1", "10.00_10s")));
    }

    @Test
    public void applyCategoryMappingShouldRejectAllBidsFromBidderInDifferentReasons() {
        // given
        final List<BidderResponse> bidderResponses = asList(
                givenBidderResponse("rubicon",
                        givenBidderBid("1", "10", BidType.video, singletonList("cat1"), 10, "prCategory1")),
                givenBidderResponse("otherBidder",
                        givenBidderBid("2", "10", BidType.video, null, 10, "prCategory1"),
                        givenBidderBid("3", "10", BidType.video, singletonList("cat1"), 30, "prCategory1")));

        final ExtRequestTargeting extRequestTargeting = givenTargeting(1, "publisher", asList(10, 15, 5), true, true);
        given(applicationSettings.getCategory(anyString(), anyString(), anyString())).willReturn(
                Future.succeededFuture("fetchedCat1"), Future.succeededFuture("fetchedCat2"),
                Future.succeededFuture("fetchedCat3"));

        // when
        final Future<CategoryMappingResult> resultFuture = categoryMapper.applyCategoryMapping(bidderResponses,
                extRequestTargeting);

        // then
        assertThat(resultFuture.succeeded()).isTrue();
        assertThat(resultFuture.result().getBidderToBidCategory())
                .isEqualTo(Collections.singletonMap("rubicon",
                        Collections.singletonMap("1", "10.00_fetchedCat1_10s")));
        assertThat(resultFuture.result().getBidderResponses())
                .extracting(BidderResponse::getSeatBid)
                .flatExtracting(BidderSeatBid::getBids)
                .extracting(BidderBid::getBid).hasSize(1)
                .extracting(Bid::getId)
                .containsOnly("1");
        assertThat(resultFuture.result().getErrors()).hasSize(2)
                .containsOnly("Bid rejected [bidder: otherBidder, bid ID: 2] with a reason: Bid did not contain "
                                + "a category",
                        "Bid rejected [bidder: otherBidder, bid ID: 3] with a reason: Bid duration '30' exceeds"
                                + " maximum '15'");
    }

    private static BidderResponse givenBidderResponse(String bidder, BidderBid... bidderBids) {
        return BidderResponse.of(bidder, BidderSeatBid.of(asList(bidderBids), null, null), 100);
    }

    private static BidderBid givenBidderBid(String bidId, String price, BidType bidType, List<String> cat,
                                            Integer duration, String primaryCategory) {
        return BidderBid.of(
                Bid.builder()
                        .id(bidId)
                        .cat(cat)
                        .price(new BigDecimal(price))
                        .build(),
                bidType, null, null, ExtBidPrebidVideo.of(duration, primaryCategory));
    }

    private static ExtRequestTargeting givenTargeting(Integer primaryAdServer,
                                                      String publisher,
                                                      List<Integer> durations,
                                                      Boolean withCategory,
                                                      Boolean translateCategories) {
        return ExtRequestTargeting.builder()
                .pricegranularity(mapper.valueToTree(ExtPriceGranularity.from(priceGranularity)))
                .includebrandcategory(ExtIncludeBrandCategory.of(primaryAdServer, publisher, withCategory,
                        translateCategories))
                .durationrangesec(durations)
                .build();
    }
}