package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:供应商数据
 * @Date 2018/5/24
 */
public abstract class AbstractSyncSupplierDataJobHandler extends JobHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier("enterpriseSpaceDataSource")
    protected DataSource enterpriseSpaceDataSource;
    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    @Qualifier("purchaseDataSource")
    protected DataSource purchaseDataSource;

    @Autowired
    @Qualifier("tenderDataSource")
    protected DataSource tenderDataSource;

    @Autowired
    @Qualifier("auctionDataSource")
    protected DataSource auctionDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    @Value("${enterpriseSpaceFormat}")
    protected String enterpriseSpaceFormat;

    @Value("${enterpriseSpaceDetailFormat}")
    protected String enterpriseSpaceDetailFormat;

    protected String ID                          = "id";
    protected String USER_ID                     = "userId";
    protected String CORE                        = "core";
    protected String AREA_STR                    = "areaStr";
    protected String AREA_STR_NOT_ANALYZED       = "areaStrNotAnalyzed";
    protected String ZONE_STR                    = "zoneStr";
    protected String AREA                        = "area";
    protected String CITY                        = "city";
    protected String COUNTY                      = "county";
    protected String CREDIT_RATING               = "creditRating";
    protected String AUTH_CODE_ID                = "authCodeId";
    protected String AUTHEN_NUMBER               = "authenNumber";
    protected String CODE                        = "code";
    protected String AREA_CODE                   = "areaCode";
    protected String FUND                        = "fund";
    protected String TENANT_ID                   = "tenantId";
    protected String MOBILE                      = "mobile";
    protected String TEL                         = "tel";
    protected String MAIN_PRODUCT                = "mainProduct";
    protected String MAIN_PRODUCT_NOT_ANALYZED   = "mainProductNotAnalyzed";
    protected String LOGIN_NAME                  = "loginName";
    protected String ENTERPRISE_SPACE            = "enterpriseSpace";
    protected String ENTERPRISE_SPACE_DETAIL     = "enterpriseSpaceDetail";
    protected String ENTERPRISE_SPACE_ACTIVE     = "enterpriseSpaceActive";
    protected String INDUSTRY_CODE               = "industryCode";
    protected String INDUSTRY_STR                = "industryStr";
    protected String TOTAL_PURCHASE_PROJECT      = "totalPurchaseProject";
    protected String TOTAL_BID_PROJECT           = "totalBidProject";
    protected String TOTAL_AUCTION_PROJECT       = "totalAuctionProject";
    protected String TOTAL_PROJECT               = "totalProject";
    protected String TOTAL_DEAL_PURCHASE_PROJECT = "totalDealPurchaseProject";
    protected String TOTAL_DEAL_BID_PROJECT      = "totalDealBidProject";
    protected String TOTAL_DEAL_AUCTION_PROJECT  = "totalDealAuctionProject";
    protected String TOTAL_DEAL_PROJECT          = "totalDealProject";
    protected String TOTAL_DEAL_PURCHASE_PRICE   = "totalDealPurchasePrice";
    protected String TOTAL_DEAL_BID_PRICE        = "totalDealBidPrice";
    protected String TOTAL_DEAL_AUCTION_PRICE    = "totalDealAuctionPrice";
    protected String TOTAL_DEAL_PRICE            = "totalDealPrice";
    protected String TOTAL_PRODUCT               = "totalProduct";
    protected String TOTAL_COOPERATED_PURCHASER  = "totalCooperatedPurchaser";
    protected String REGION                      = "region";
    protected String COMPANY_NAME                = "companyName";
    protected String COMPANY_NAME_NOT_ANALYZED   = "companyNameNotAnalyzed";
    protected String DATA_STATUS                 = "dataStatus";

    // 两位有效数字，四舍五入
    protected final DecimalFormat format = new DecimalFormat("0.00");

    // 行业编号正则
    protected final Pattern number = Pattern.compile("\\d+");

    protected Map<String, String> industryCodeMap = new HashMap<>();

    protected void batchExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate);
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier"),
                                String.valueOf(result.get(ID)))
                        .setSource(JSON.toJSONString(result, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {
                                    return propertyValue;
                                }
                            }
                        })));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    protected void batchAndUpdateExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate);
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareUpdate(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier"),
                                String.valueOf(result.get(ID)))
                        .setDocAsUpsert(true)
                        .setDoc(JSON.toJSONString(result, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {
                                    return propertyValue;
                                }
                            }
                        })));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    protected void batchUpdateExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate);
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareUpdate(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.supplier"),
                                String.valueOf(result.get(ID)))
                        .setDoc(JSON.toJSONString(result, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {
                                    return propertyValue;
                                }
                            }
                        })));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }
}
