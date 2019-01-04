package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
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
import java.util.Objects;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/5/15
 */
public abstract class AbstractSyncYcNoticeDataJobHandler extends JobHandler {

    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "ycDataSource")
    protected DataSource ycDataSource;

    protected String ID                        = "id";
    protected String PROJECT_ID                = "projectId";
    protected String COMPANY_ID                = "companyId";
    protected String SYNC_TIME                 = "syncTime";
    protected String PROJECT_TYPE              = "projectType";
    protected String COMPANY_NAME_NOT_ANALYZED = "companyNameNotAnalyzed";
    protected String COMPANY_NAME              = "companyName";
    protected String PROJECT_NAME_NOT_ANALYZED = "projectNameNotAnalyzed";
    protected String PROJECT_NAME              = "projectName";
    protected String NOTICE_TYPE               = "noticeType";
    protected String SUB_PROJECT_ID            = "subProjectId";
    protected String LINK_PHONE                = "linkPhone";
    protected String LINK_TEL                  = "linkTel";
    protected String IS_SHOW_MOBILE            = "isShowMobile";
    protected String IS_SHOW_TEL               = "isShowTel";
    protected String OPT_STATUS                = "optStatus";   // -1:删除状态

    protected Integer SOURCE_NOTICE        = 1; // 原始公告和变更公告
    protected Integer RESULT_NOTICE        = 2; // 结果公告
    protected Integer BID_NOTICE_TYPE      = 1; // 招标公告
    protected Integer PURCHASE_NOTICE_TYPE = 2; // 采购公告
    protected Integer AUCTION_NOTICE_TYPE  = 3; // 竞价公告


    protected void doSyncNoticeService(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql: {}, params: {}, 共{}条", countSql, params, count);
        for (long i = 0; i < count; i = i + pageSize) {
            // 添加分页
            List<Object> paramsToUse = appendToParams(params, i);
            List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
            logger.debug("执行querySql: {}, params: {}, 共{}条", querySql, paramsToUse, mapList.size());
            for (Map<String, Object> result : mapList) {
                refresh(result);
            }
            batchExecute(mapList);
        }
    }


    protected void refresh(Map<String, Object> result) {
        // 处理 id projectId companyId为String类型
        Object projectType = result.get(PROJECT_TYPE);
        if (Objects.equals(projectType, "8")) {
            result.put(PROJECT_TYPE, PURCHASE_NOTICE_TYPE);
            // 采购项目
            if (Objects.equals(result.get(NOTICE_TYPE), "2903")) {
                // 结果公告
                result.put(NOTICE_TYPE, RESULT_NOTICE);
            } else {
                result.put(NOTICE_TYPE, SOURCE_NOTICE);
            }
        } else if (Objects.equals(projectType, "7")) {
            // 竞价项目 悦采竞价项目默认公开联系电话和固话
            result.put(PROJECT_TYPE, AUCTION_NOTICE_TYPE);
            result.put(IS_SHOW_MOBILE, 1);
            result.put(LINK_PHONE, result.get(LINK_TEL));
            result.remove(LINK_TEL);
            result.remove(IS_SHOW_TEL);
            if (Objects.equals(result.get(NOTICE_TYPE), "3")) {
                // 结果公告
                result.put(NOTICE_TYPE, RESULT_NOTICE);
            } else {
                result.put(NOTICE_TYPE, SOURCE_NOTICE);
            }
        } else if (Objects.equals(projectType, "1")) {
            // 招标项目
            result.put(PROJECT_TYPE, BID_NOTICE_TYPE);
            result.put(SUB_PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
            if (Objects.equals(result.get(NOTICE_TYPE), "0108")) {
                // 中标公告
                result.put(NOTICE_TYPE, RESULT_NOTICE);
            } else {
                result.put(NOTICE_TYPE, SOURCE_NOTICE);
            }
        }
        result.put(ID, String.valueOf(result.get(ID)));
        result.put(COMPANY_ID, String.valueOf(result.get(COMPANY_ID)));
        result.put(PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
        result.put(PROJECT_NAME_NOT_ANALYZED, result.get(PROJECT_NAME));
        result.put(COMPANY_NAME_NOT_ANALYZED, result.get(COMPANY_NAME));
        // 添加同步时间字段
        result.put(SYNC_TIME, SyncTimeUtil.getCurrentDate());
        //添加平台来源
        result.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
    }

    protected void batchExecute(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.notice"),
                                String.valueOf(map.get(ID)))
                        .setSource(JSON.toJSONString(map, new ValueFilter() {
                            @Override
                            public Object process(Object o, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {
                                    return propertyValue;
                                }
                            }
                        })));
            }
            BulkResponse responses = bulkRequest.execute().actionGet();
            if (responses.hasFailures()) {
                logger.error(responses.buildFailureMessage());
            }
        }
    }
}
