package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步新平台采购公告
 * @Date 2018/3/16
 */
@Service
@JobHander("syncPurchaseNoticeDataJobHandler")
public class SyncPurchaseNoticeDataJobHandler extends AbstractSyncNoticeDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步新平台采购公告开始");
        syncPurchaseNoticeData();
        logger.info("同步新平台采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.notice",
                QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_NOTICE_TYPE));
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
                "\t`purchase_notice_history` pnh\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnh.project_id = ppc.id \n" +
                "\tAND pnh.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.project_open_range = 1 \n" +
                "\tAND pnh.create_time >?";
        String querySql = "SELECT\n" +
                "\tpnh.id,\n" +
                "\tpnh.project_id AS projectId,\n" +
                "\tpnh.project_name AS projectName,\n" +
                "\tpnh.project_code AS projectCode,\n" +
                "\tpnh.quote_stop_time AS quoteStopTime,\n" +
                "\tpnh.publish_notice_time AS publishNoticeTime,\n" +
                "\tpnh.project_info AS projectInfo,\n" +
                "\tpnh.link_man AS linkMan,\n" +
                "\tpnh.link_phone AS linkPhone,\n" +
                "\tpnh.link_tel AS linkTel,\n" +
                "\tpnh.link_mail AS linkMail,\n" +
                "\tpnh.is_show_tel AS isShowTel,\n" +
                "\tpnh.is_show_mobile AS isShowMobile,\n" +
                "\tpnh.company_id AS companyId,\n" +
                "\tpnh.create_time AS createTime,\n" +
                "\tpnh.company_name AS companyName,\n" +
                "\tppc.result_open_range AS resultOpenRange,\n" +
                "\tppc.price_open_range AS priceOpenRange \n" +
                "FROM\n" +
                "\t`purchase_notice_history` pnh\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnh.project_id = ppc.id \n" +
                "\tAND pnh.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.project_open_range = 1 \n" +
                "\tAND pnh.create_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(purchaseDataSource, countSql, querySql, params, SOURCE_NOTICE);
    }

    private void syncUpdatePurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_history` pnh\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnh.project_id = ppc.id \n" +
                "\tAND pnh.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.result_open_range = 1 \n" +
                "\tAND pnh.update_time >?";
        String querySql = "SELECT\n" +
                "\tpnh.id,\n" +
                "\tpnh.project_id AS projectId,\n" +
                "\tpnh.project_name AS projectName,\n" +
                "\tpnh.project_code AS projectCode,\n" +
                "\tpnh.quote_stop_time AS quoteStopTime,\n" +
                "\tpnh.publish_notice_time AS publishNoticeTime,\n" +
                "\tpnh.project_info AS projectInfo,\n" +
                "\tpnh.link_man AS linkMan,\n" +
                "\tpnh.link_phone AS linkPhone,\n" +
                "\tpnh.link_tel AS linkTel,\n" +
                "\tpnh.link_mail AS linkMail,\n" +
                "\tpnh.is_show_tel AS isShowTel,\n" +
                "\tpnh.is_show_mobile AS isShowMobile,\n" +
                "\tpnh.company_id AS companyId,\n" +
                "\tpnh.create_time AS createTime,\n" +
                "\tpnh.company_name AS companyName,\n" +
                "\tppc.result_open_range AS resultOpenRange,\n" +
                "\tppc.price_open_range AS priceOpenRange \n" +
                "FROM\n" +
                "\t`purchase_notice_history` pnh\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnh.project_id = ppc.id \n" +
                "\tAND pnh.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.result_open_range = 1 \n" +
                "\tAND pnh.update_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(purchaseDataSource, countSql, querySql, params, SOURCE_NOTICE);
    }

    private void syncInsertPurchaseResultNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_result` pnr\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnr.project_id = ppc.id \n" +
                "\tAND pnr.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.result_open_range = 1\n" +
                "\tand pnr.create_time > ?";
        String querySql = "SELECT\n" +
                "\tpnr.id,\n" +
                "\tpnr.project_id AS projectId,\n" +
                "\tpnr.project_name AS projectName,\n" +
                "\tpnr.project_code AS projectCode,\n" +
                "\tpnr.quote_stop_time AS quoteStopTime,\n" +
                "\tpnr.publish_notice_time AS publishNoticeTime,\n" +
                "\tpnr.publish_result_time AS publishResultTime,\n" +
                "\tpnr.project_info AS projectInfo,\n" +
                "\tpnr.link_man AS linkMan,\n" +
                "\tpnr.link_phone AS linkPhone,\n" +
                "\tpnr.link_tel AS linkTel,\n" +
                "\tpnr.link_mail AS linkMail,\n" +
                "\tpnr.is_show_tel AS isShowTel,\n" +
                "\tpnr.is_show_mobile AS isShowMobile,\n" +
                "\tpnr.company_id AS companyId,\n" +
                "\tpnr.create_time AS createTime,\n" +
                "\tpnr.company_name AS companyName,\n" +
                "\tppc.result_open_range AS resultOpenRange, \n" +
                "\tppc.price_open_range AS priceOpenRange\n" +
                "FROM\n" +
                "\t`purchase_notice_result` pnr\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnr.project_id = ppc.id and pnr.company_id = ppc.company_id\n" +
                "WHERE\n" +
                "\tppc.result_open_range = 1\n" +
                "\tand pnr.create_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(purchaseDataSource, countSql, querySql, params, RESULT_NOTICE);
    }

    private void syncUpdatePurchaseResultNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`purchase_notice_result` pnr\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnr.project_id = ppc.id \n" +
                "\tAND pnr.company_id = ppc.company_id \n" +
                "WHERE\n" +
                "\tppc.project_open_range = 1\n" +
                "\tand pnr.update_time > ?";
        String querySql = "SELECT\n" +
                "\tpnr.id,\n" +
                "\tpnr.project_id AS projectId,\n" +
                "\tpnr.project_name AS projectName,\n" +
                "\tpnr.project_code AS projectCode,\n" +
                "\tpnr.quote_stop_time AS quoteStopTime,\n" +
                "\tpnr.publish_notice_time AS publishNoticeTime,\n" +
                "\tpnr.publish_result_time AS publishResultTime,\n" +
                "\tpnr.project_info AS projectInfo,\n" +
                "\tpnr.link_man AS linkMan,\n" +
                "\tpnr.link_phone AS linkPhone,\n" +
                "\tpnr.link_tel AS linkTel,\n" +
                "\tpnr.link_mail AS linkMail,\n" +
                "\tpnr.is_show_tel AS isShowTel,\n" +
                "\tpnr.is_show_mobile AS isShowMobile,\n" +
                "\tpnr.company_id AS companyId,\n" +
                "\tpnr.create_time AS createTime,\n" +
                "\tpnr.company_name AS companyName,\n" +
                "\tppc.result_open_range AS resultOpenRange, \n" +
                "\tppc.price_open_range AS priceOpenRange\n" +
                "FROM\n" +
                "\t`purchase_notice_result` pnr\n" +
                "\tLEFT JOIN purchase_project_control ppc ON pnr.project_id = ppc.id and pnr.company_id = ppc.company_id\n" +
                "WHERE\n" +
                "\tppc.project_open_range = 1\n" +
                "\tand pnr.update_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(purchaseDataSource, countSql, querySql, params, SOURCE_NOTICE);

    }

    @Override
    protected void refresh(Map<String, Object> map) {
        super.refresh(map);

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
        // 公告类型为采购公告
        map.put(PROJECT_TYPE, PURCHASE_NOTICE_TYPE);

    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
