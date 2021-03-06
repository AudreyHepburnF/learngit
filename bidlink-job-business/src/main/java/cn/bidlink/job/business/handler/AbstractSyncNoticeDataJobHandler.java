package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/5/15
 */
public abstract class AbstractSyncNoticeDataJobHandler extends JobHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "purchaseDataSource")
    protected DataSource purchaseDataSource;

    @Autowired
    @Qualifier("tenderDataSource")
    protected DataSource tenderDataSource;

    @Autowired
    @Qualifier("auctionDataSource")
    protected DataSource auctionDataSource;

    @Autowired
    @Qualifier("vendueDataSource")
    protected DataSource vendueDataSource;

    @Autowired
    @Qualifier("apiDataSource")
    protected DataSource apiDataSource;

    protected String ID                        = "id";
    protected String PROJECT_ID                = "projectId";
    protected String COMPANY_ID                = "companyId";
    protected String SYNC_TIME                 = "syncTime";
    protected String IS_SHOW_TEL               = "isShowTel";
    protected String IS_SHOW_MOBILE            = "isShowMobile";
    protected String PROJECT_TYPE              = "projectType";
    protected String COMPANY_NAME_NOT_ANALYZED = "companyNameNotAnalyzed";
    protected String COMPANY_NAME              = "companyName";
    protected String PROJECT_NAME_NOT_ANALYZED = "projectNameNotAnalyzed";
    protected String PROJECT_NAME              = "projectName";
    protected String NOTICE_TYPE               = "noticeType";
    protected String SUB_PROJECT_ID            = "subProjectId";
    protected String APPROVE_STATUS            = "approveStatus";
    protected String RESULT_OPEN_RANGE         = "resultOpenRange";
    protected String PRICE_OPEN_RANGE          = "priceOpenRange";

    protected Integer SOURCE_NOTICE        = 1; // 原始公告和变更公告
    protected Integer RESULT_NOTICE        = 2; // 结果公告
    protected Integer BID_NOTICE_TYPE      = 1; // 招标公告
    protected Integer PURCHASE_NOTICE_TYPE = 2; // 采购公告
    protected Integer AUCTION_NOTICE_TYPE  = 3; // 竞价公告
    protected Integer SALE_NOTICE_TYPE     = 5; // 拍卖公告


    protected void doSyncNoticeService(DataSource dataSource, String countSql, String querySql, List<Object> params, Integer noticeType) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql: {}, params: {}, 共{}条", countSql, params, count);
        for (long i = 0; i < count; i = i + pageSize) {
            // 添加分页
            List<Object> paramsToUse = appendToParams(params, i);
            List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
            logger.debug("执行querySql: {}, params: {}, 共{}条", querySql, paramsToUse, mapList.size());
            for (Map<String, Object> result : mapList) {
                result.put(NOTICE_TYPE, noticeType);
                refresh(result);
            }
            batchExecute(mapList);
        }
    }


    protected void refresh(Map<String, Object> result) {
        // 处理 id projectId companyId为String类型
        result.put(ID, String.valueOf(result.get(ID)));
        result.put(COMPANY_ID, String.valueOf(result.get(COMPANY_ID)));
        result.put(PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
        result.put(PROJECT_NAME_NOT_ANALYZED, result.get(PROJECT_NAME));
        result.put(COMPANY_NAME_NOT_ANALYZED, result.get(COMPANY_NAME));
        // 添加同步时间字段
        result.put(SYNC_TIME, SyncTimeUtil.getCurrentDate());
        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
    }

    protected void batchExecute(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.notice_index"),
                                elasticClient.getProperties().getProperty("cluster.type.notice"),
                                String.valueOf(map.get(ID)))
                        .setSource(SyncTimeUtil.handlerDate(map)));
            }
            BulkResponse responses = bulkRequest.execute().actionGet();
            if (responses.hasFailures()) {
                logger.error(responses.buildFailureMessage());
            }
        }
    }
}
