package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步新平台采购公告
 * @Date 2018/3/16
 */
@Service
@JobHander("syncXieTongPurchaseNoticeDataJobHandler")
public class SyncXieTongPurchaseNoticeDataJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncXieTongPurchaseNoticeDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "purchaseDataSource")
    private DataSource purchaseDataSource;

    @Value("${pageSize}")
    private Integer pageSize;

    private String  ID             = "id";
    private String  PROJECT_ID     = "projectId";
    private String  COMPANY_ID     = "companyId";
    private String  SYNC_TIME      = "syncTime";
    private String  IS_SHOW_TEL    = "isShowTel";
    private String  IS_SHOW_MOBILE = "isShowMobile";
    private Integer SOURCE_NOTICE  = 1;
    private Integer RESULT_NOTICE  = 2;


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步新平台采购公告开始");
        syncPurchaseNoticeData();
        logger.info("同步新平台采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.purchase_notice", null);
        logger.info("同步新平台采购公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncPurchaseUnderwayNoticeService(lastSyncTime);
        syncPurchaseResultNoticeService(lastSyncTime);
    }

    private void syncPurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        logger.info("同步原始公告和变更公告开始");
        syncInsertPurchaseUnderwayNoticeService(lastSyncTime);
        syncUpdatePurchaseUnderwayNoticeService(lastSyncTime);
        logger.info("同步原始公告和变更公告结束");
    }

    private void syncPurchaseResultNoticeService(Timestamp lastSyncTime) {
        logger.info("同步结果公告开始");
        syncInsertPurchaseResultNoticeService(lastSyncTime);
        syncUpdatePurchaseResultNoticeService(lastSyncTime);
        logger.info("同步结果公告结束");
    }

    private void syncInsertPurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_history` \n" +
                "WHERE\n" +
                "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tproject_name AS projectNameAlias,\n" +
                "\tproject_code AS projectCode,\n" +
                "\tquote_stop_time AS quoteStopTime,\n" +
                "\tpublish_notice_time AS publishNoticeTime,\n" +
                "\tproject_info AS projectInfo,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tis_show_tel AS isShowTel,\n" +
                "\tis_show_mobile AS isShowMobile,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_name AS companyNameAlias \n" +
                "FROM\n" +
                "\t`purchase_notice_history`\n" +
                "\tWHERE\n" +
                "\tcreate_time > ?\n" +
                "\torder by id ASC\n" +
                "\tlimit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaseNoticeService(countSql, querySql, params, SOURCE_NOTICE);
    }

    private void syncUpdatePurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_history` \n" +
                "WHERE\n" +
                "\tupdate_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tproject_name AS projectNameAlias,\n" +
                "\tproject_code AS projectCode,\n" +
                "\tquote_stop_time AS quoteStopTime,\n" +
                "\tpublish_notice_time AS publishNoticeTime,\n" +
                "\tproject_info AS projectInfo,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tis_show_tel AS isShowTel,\n" +
                "\tis_show_mobile AS isShowMobile,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_name AS companyNameAlias \n" +
                "FROM\n" +
                "\t`purchase_notice_history`\n" +
                "\tWHERE\n" +
                "\tupdate_time > ?\n" +
                "\tlimit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaseNoticeService(countSql, querySql, params, SOURCE_NOTICE);
    }

    private void syncInsertPurchaseResultNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_result` \n" +
                "WHERE\n" +
                "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tproject_name AS projectNameAlias,\n" +
                "\tproject_code AS projectCode,\n" +
                "\tquote_stop_time AS quoteStopTime,\n" +
                "\tpublish_notice_time AS publishNoticeTime,\n" +
                "\tpublish_result_time AS publishResultTime,\n" +
                "\tproject_info AS projectInfo,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tis_show_tel AS isShowTel,\n" +
                "\tis_show_mobile AS isShowMobile,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_name AS companyNameAlias \n" +
                "FROM\n" +
                "\t`purchase_notice_result`\n" +
                "\tWHERE\n" +
                "\tcreate_time > ?\n" +
                "\tlimit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaseNoticeService(countSql, querySql, params, RESULT_NOTICE);
    }

    private void syncUpdatePurchaseResultNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_result` \n" +
                "WHERE\n" +
                "\tupdate_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tproject_name AS projectNameAlias,\n" +
                "\tproject_code AS projectCode,\n" +
                "\tquote_stop_time AS quoteStopTime,\n" +
                "\tpublish_notice_time AS publishNoticeTime,\n" +
                "\tpublish_result_time AS publishResultTime,\n" +
                "\tproject_info AS projectInfo,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tis_show_tel AS isShowTel,\n" +
                "\tis_show_mobile AS isShowMobile,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_name AS companyNameAlias \n" +
                "FROM\n" +
                "\t`purchase_notice_result`\n" +
                "\tWHERE\n" +
                "\tupdate_time > ?\n" +
                "\tlimit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaseNoticeService(countSql, querySql, params, SOURCE_NOTICE);

    }

    private void doSyncPurchaseNoticeService(String countSql, String querySql, ArrayList<Object> params, Integer noticeType) {
        long count = DBUtil.count(purchaseDataSource, countSql, params);
        logger.debug("执行countSql:{} , params:{} , 共{}条", countSql, params, count);
        for (long i = 0; i < count; i = i + pageSize) {
            ArrayList<Object> paramsToUse = paramsToUse(params, i);
            List<Map<String, Object>> mapList = DBUtil.query(purchaseDataSource, querySql, paramsToUse);
            logger.debug("执行querySql:{} , params:{}, 共{}条", querySql, paramsToUse, mapList.size());
            for (Map<String, Object> map : mapList) {
                // 公告类型 原始公告和变更公告为:1  结果公告为:2
                map.put("noticeType", noticeType);
                refresh(map);
            }
            batchExecute(mapList);
        }
    }

    private void batchExecute(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.purchase_notice"),
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

    private void refresh(Map<String, Object> map) {
        // 数据类型转换
        map.put(ID, String.valueOf(map.get(ID)));
        map.put(COMPANY_ID, String.valueOf(map.get(COMPANY_ID)));
        map.put(PROJECT_ID, String.valueOf(map.get(PROJECT_ID)));

        Boolean isShowMobile = (Boolean) map.get(IS_SHOW_MOBILE);
        if (isShowMobile != null && isShowMobile) {
            map.put(IS_SHOW_MOBILE, 1);
        } else {
            map.put(IS_SHOW_MOBILE, 0);
        }
        Boolean isShowTel = (Boolean) map.get(IS_SHOW_TEL);
        if (isShowTel != null && isShowTel) {
            map.put(IS_SHOW_TEL, 1);
        } else {
            map.put(IS_SHOW_TEL, 0);
        }
        // 添加同步时间字段
        map.put(SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    private ArrayList<Object> paramsToUse(ArrayList<Object> params, long i) {
        ArrayList<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
