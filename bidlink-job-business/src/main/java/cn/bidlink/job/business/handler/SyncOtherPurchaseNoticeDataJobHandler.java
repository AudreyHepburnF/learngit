package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步第三方平台采购公告
 * @Date 2018/3/16
 */
@Service
@JobHander("syncOtherPurchaseNoticeDataJobHandler")
public class SyncOtherPurchaseNoticeDataJobHandler extends AbstractSyncNoticeDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步第三方采购公告开始");
        syncPurchaseNoticeData();
        logger.info("同步第三方采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.notice_index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_NOTICE_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE)));
//        Timestamp lastSyncTime = SyncTimeUtil.GMT_TIME;
        logger.info("同步第三方采购公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncPurchaseNoticeService(lastSyncTime);
    }

    private void syncPurchaseNoticeService(Timestamp lastSyncTime) {
        logger.info("同步原始公告和变更公告开始");
        syncPurchaseUnderwayNoticeService(lastSyncTime);
        logger.info("同步原始公告和变更公告结束");

        logger.info("同步结果公告开始");
        syncPurchaseResultNoticeService(lastSyncTime);
        logger.info("同步结果公告结束");
    }

    private void syncPurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_information_history` pih \n" +
                "WHERE\n" +
                "\tpih.create_time > ?";
        String querySql = "SELECT\n" +
                "\tpih.id,\n" +
                "\tpih.project_id AS projectId,\n" +
                "\tpih.NAME AS projectName,\n" +
                "\tpih.CODE AS projectCode,\n" +
                "\tpih.quote_stop_time AS quoteStopTime,\n" +
                "\tpih.publish_notice_time AS publishNoticeTime,\n" +
                "\tpih.project_info AS projectInfo,\n" +
                "\tpih.link_man AS linkMan,\n" +
                "\tpih.link_phone AS linkPhone,\n" +
                "\tpih.link_tel AS linkTel,\n" +
                "\tpih.link_mail AS linkMail,\n" +
                "\tpih.company_id AS companyId,\n" +
                "\tpih.create_time AS createTime,\n" +
                "\tpih.bid_url AS bidUrl,\n" +
                "\tpih.company_name AS companyName \n" +
                "FROM\n" +
                "\t`purchase_information_history` pih \n" +
                "WHERE\n" +
                "\tpih.create_time > ? \n" +
                "\tLIMIT ?,?";
        doSyncNoticeService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), SOURCE_NOTICE);
    }

    private void syncPurchaseResultNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tCOUNT( DISTINCT ( pi.id ) ) \n" +
                "FROM\n" +
                "\tpurchase_information pi\n" +
                "\tINNER JOIN purchase_information_result pir ON pi.id = pir.project_id \n" +
                "WHERE\n" +
                "\tpi.update_time > ?";
        String querySql = "SELECT\n" +
                "\tpir.id AS id,\n" +
                "\tpi.id AS projectId,\n" +
                "\tpi.NAME AS projectName,\n" +
                "\tpi.CODE AS projectCode,\n" +
                "\tpi.quote_stop_time AS quoteStopTime,\n" +
                "\tpi.publish_notice_time AS publishNoticeTime,\n" +
                "\tpi.deal_time AS publishResultTime,\n" +
                "\tpi.link_man AS linkMan,\n" +
                "\tpi.link_phone AS linkPhone,\n" +
                "\tpi.link_tel AS linkTel,\n" +
                "\tpi.link_mail AS linkMail,\n" +
                "\tpir.create_time AS createTime,\n" +
                "\tpi.bid_url AS bidUrl,\n" +
                "\tpir.company_id AS companyId,\n" +
                "\tpi.company_name AS companyName\n" +
                "\t\n" +
                "FROM\n" +
                "\tpurchase_information pi\n" +
                "\tINNER JOIN purchase_information_result pir ON pi.id = pir.project_id\n" +
                "\tWHERE pi.update_time > ?\n" +
                "GROUP BY\n" +
                "\tpi.id\n" +
                "LIMIT ?,?";
        doSyncNoticeService(apiDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), RESULT_NOTICE);
    }

    @Override
    protected void refresh(Map<String, Object> map) {
        super.refresh(map);
        // 其他平台默认为公开
        map.put(IS_SHOW_MOBILE, 1);
        map.put(IS_SHOW_TEL, 1);
        map.put(PRICE_OPEN_RANGE, 1);
        map.put(RESULT_OPEN_RANGE, 1);
        map.put(PROJECT_TYPE, PURCHASE_NOTICE_TYPE);
        map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.OTHER_SOURCE);
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
