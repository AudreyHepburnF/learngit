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
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/5/24
 */
public abstract class AbstractSyncPurchaseDataJobHandler extends JobHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected ElasticClient elasticClient;

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
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    protected String ID                         = "id";
    protected String COMPANY_ID                 = "companyId";
    protected String PURCHASE_TRADING_VOLUME    = "purchaseTradingVolume";
    protected String BID_TRADING_VOLUME         = "bidTradingVolume";
    protected String AUCTION_TRADING_VOLUME_STR = "auctionTradingVolumeStr";
    protected String TRADING_VOLUME             = "tradingVolume";
    protected String LONG_TRADING_VOLUME        = "longTradingVolume";
    protected String COMPANY_SITE_ALIAS         = "companySiteAlias";
    protected String PURCHASE_PROJECT_COUNT     = "purchaseProjectCount";
    protected String BID_PROJECT_COUNT          = "bidProjectCount";
    protected String AUCTION_PROJECT_COUNT      = "auctionProjectCount";
    protected String PROJECT_COUNT              = "projectCount";
    protected String REGION                     = "region";
    protected String AREA_STR                   = "areaStr";
    protected String AREA_STR_NOT_ANALYZED      = "areaStrNotAnalyzed";
    protected String INDUSTRY_STR               = "industryStr";
    protected String INDUSTRY_STR_NOT_ANALYZED  = "industryStrNotAnalyzed";
    protected String ZONE_STR                   = "zoneStr";
    protected String ZONE_STR_NOT_ANALYZED      = "zoneStrNotAnalyzed";
    protected String PURCHASE_NAME              = "purchaseName";
    protected String PURCHASE_NAME_NOT_ANALYZED = "purchaseNameNotAnalyzed";
    protected String COOPERATE_SUPPLIER_COUNT   = "cooperateSupplierCount";
    protected String DATA_STATUS                = "dataStatus";

    protected void batchInsert(List<Map<String, Object>> purchases) {
//        System.out.println("=============" + purchases);
        if (!CollectionUtils.isEmpty(purchases)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> purchase : purchases) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.purchase"),
                                String.valueOf(purchase.get(ID)))
                        .setSource(JSON.toJSONString(purchase, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    //是date类型按指定日期格式转换
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {

                                    return propertyValue;
                                }
                            }
                        })));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            //是否失败
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }
}
