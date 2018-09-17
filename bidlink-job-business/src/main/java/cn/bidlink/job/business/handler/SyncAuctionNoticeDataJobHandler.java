package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:竞价公告定时同步任务
 * @Date 2018/9/17
 */
@Service
@JobHander(value = "syncAuctionNoticeDataJobHandler")
public class SyncAuctionNoticeDataJobHandler extends AbstractSyncNoticeDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步竞价公告");
        syncAuctionNoticeData();
        logger.info("结束同步竞价公告");
        return ReturnT.SUCCESS;
    }

    private void syncAuctionNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, AUCTION_NOTICE_TYPE)));
        logger.info("同步竞价公告lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        syncAuctionUnderWayNoticeDataService(lastSyncTime);
        syncAuctionResultNoticeDataService(lastSyncTime);
    }

    private void syncAuctionResultNoticeDataService(Timestamp lastSyncTime) {
        logger.info("开始同步竞价项目结果公告");
        syncAuctionInsertResultNoticeDataService(lastSyncTime);
        syncAuctionUpdateResultNoticeDataService(lastSyncTime);
        logger.info("结果同步竞价项目结果公告");
    }

    private void syncAuctionUnderWayNoticeDataService(Timestamp lastSyncTime) {
        logger.info("开始同步竞价项目原始公告和变更公告");
        syncAuctionInsertUnderWayNoticeDataService(lastSyncTime);
        syncAuctionUpdateUnderWayNoticeDataService(lastSyncTime);
        logger.info("结束同步竞价项目原始公告和变更公告");
    }

    private void syncAuctionInsertUnderWayNoticeDataService(Timestamp lastSyncTime) {
        String countSql = " SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tauction_notice_history \n" +
                "WHERE\n" +
                "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tanh.id,\n" +
                "\tanh.project_id AS projectId,\n" +
                "\tanh.project_name AS projectName,\n" +
                "\tanh.project_code AS projectCode,\n" +
                // TODO 竞价项目的截止时间待产品定义
//                "\tanh.auction_end_time AS quoteStopTime,\n" +
                "\tanh.publish_notice_time AS publishNoticeTime,\n" +
                "\tanh.project_info AS projectInfo,\n" +
                "\tanh.link_man AS linkMan,\n" +
                "\tanh.link_phone AS linkPhone,\n" +
                "\tanh.link_tel AS linkTel,\n" +
                "\tanh.link_mail AS linkMail,\n" +
                "\tanh.is_show_tel AS isShowTel,\n" +
                "\tanh.is_show_mobile AS isShowMobile,\n" +
                "\tanh.company_id AS companyId,\n" +
                "\tanh.create_time AS createTime,\n" +
                "\tanh.company_name AS companyName,\n" +
                "\tanhf.file_name AS fileName,\n" +
                "\tanhf.md5 \n" +
                "FROM\n" +
                "\t`auction_notice_history` anh\n" +
                "\tLEFT JOIN auction_notice_history_file anhf ON anh.id = anhf.notice_history_id \n" +
                "WHERE\n" +
                "\tanh.create_time > ? \n" +
                "\tLIMIT ?,?";
        doSyncNoticeService(auctionDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), SOURCE_NOTICE);
    }

    private void syncAuctionUpdateUnderWayNoticeDataService(Timestamp lastSyncTime) {
        String countSql = " SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tauction_notice_history \n" +
                "WHERE\n" +
                "\tupdate_time > ?";
        String querySql = "SELECT\n" +
                "\tanh.id,\n" +
                "\tanh.project_id AS projectId,\n" +
                "\tanh.project_name AS projectName,\n" +
                "\tanh.project_code AS projectCode,\n" +
                // TODO 竞价项目的截止时间待产品定义
//                "\tanh.auction_end_time AS quoteStopTime,\n" +
                "\tanh.publish_notice_time AS publishNoticeTime,\n" +
                "\tanh.project_info AS projectInfo,\n" +
                "\tanh.link_man AS linkMan,\n" +
                "\tanh.link_phone AS linkPhone,\n" +
                "\tanh.link_tel AS linkTel,\n" +
                "\tanh.link_mail AS linkMail,\n" +
                "\tanh.is_show_tel AS isShowTel,\n" +
                "\tanh.is_show_mobile AS isShowMobile,\n" +
                "\tanh.company_id AS companyId,\n" +
                "\tanh.create_time AS createTime,\n" +
                "\tanh.company_name AS companyName,\n" +
                "\tanhf.file_name AS fileName,\n" +
                "\tanhf.md5 \n" +
                "FROM\n" +
                "\t`auction_notice_history` anh\n" +
                "\tLEFT JOIN auction_notice_history_file anhf ON anh.id = anhf.notice_history_id \n" +
                "WHERE\n" +
                "\tanh.update_time > ? \n" +
                "\tLIMIT ?,?";
        doSyncNoticeService(auctionDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), SOURCE_NOTICE);
    }

    private void syncAuctionInsertResultNoticeDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`auction_notice_result` anr\n" +
                "\tLEFT JOIN auction_project_control apc ON anr.project_id = apc.id \n" +
                "\tAND anr.company_id = apc.company_id \n" +
                "WHERE\n" +
                "\tapc.result_open_range = 1 \n" +
                "\tAND anr.create_time > ?";
        String querySql = "SELECT\n" +
                "\tanr.id,\n" +
                "\tanr.project_id AS projectId,\n" +
                "\tanr.project_name AS projectName,\n" +
                "\tanr.project_code AS projectCode,\n" +
                // TODO 竞价项目的截止时间待产品定义
//                "\tanr.auction_end_time AS quoteStopTime,\n" +
                "\tanr.publish_notice_time AS publishNoticeTime,\n" +
                "\tanr.publish_result_time AS publishResultTime,\n" +
                "\tanr.project_info AS projectInfo,\n" +
                "\tanr.link_man AS linkMan,\n" +
                "\tanr.link_phone AS linkPhone,\n" +
                "\tanr.link_tel AS linkTel,\n" +
                "\tanr.link_mail AS linkMail,\n" +
                "\tanr.is_show_tel AS isShowTel,\n" +
                "\tanr.is_show_mobile AS isShowMobile,\n" +
                "\tanr.company_id AS companyId,\n" +
                "\tanr.create_time AS createTime,\n" +
                "\tanr.company_name AS companyName,\n" +
                "\tapc.result_open_range AS resultOpenRange,\n" +
                "\tapf.file_name AS fileName,\n" +
                "\tapf.md5 \n" +
                "FROM\n" +
                "\t`auction_notice_result` anr\n" +
                "\tLEFT JOIN auction_project_control apc ON anr.project_id = apc.id \n" +
                "\tAND anr.company_id = apc.company_id\n" +
                "\tLEFT JOIN auction_project_file apf ON apf.project_id = apc.id \n" +
                "\tAND apf.company_id = apc.company_id \n" +
                "WHERE\n" +
                "\tapc.result_open_range = 1 \n" +
                "\tAND anr.create_time > ? \n" +
                "\tLIMIT ?,?;";
        doSyncNoticeService(auctionDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), RESULT_NOTICE);
    }

    private void syncAuctionUpdateResultNoticeDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`auction_notice_result` anr\n" +
                "\tLEFT JOIN auction_project_control apc ON anr.project_id = apc.id \n" +
                "\tAND anr.company_id = apc.company_id \n" +
                "WHERE\n" +
                "\tapc.result_open_range = 1 \n" +
                "\tAND anr.update_time > ?";
        String querySql = "SELECT\n" +
                "\tanr.id,\n" +
                "\tanr.project_id AS projectId,\n" +
                "\tanr.project_name AS projectName,\n" +
                "\tanr.project_code AS projectCode,\n" +
                // TODO 竞价项目的截止时间待产品定义
//                "\tanr.auction_end_time AS quoteStopTime,\n" +
                "\tanr.publish_notice_time AS publishNoticeTime,\n" +
                "\tanr.publish_result_time AS publishResultTime,\n" +
                "\tanr.project_info AS projectInfo,\n" +
                "\tanr.link_man AS linkMan,\n" +
                "\tanr.link_phone AS linkPhone,\n" +
                "\tanr.link_tel AS linkTel,\n" +
                "\tanr.link_mail AS linkMail,\n" +
                "\tanr.is_show_tel AS isShowTel,\n" +
                "\tanr.is_show_mobile AS isShowMobile,\n" +
                "\tanr.company_id AS companyId,\n" +
                "\tanr.create_time AS createTime,\n" +
                "\tanr.company_name AS companyName,\n" +
                "\tapc.result_open_range AS resultOpenRange,\n" +
                "\tapf.file_name AS fileName,\n" +
                "\tapf.md5 \n" +
                "FROM\n" +
                "\t`auction_notice_result` anr\n" +
                "\tLEFT JOIN auction_project_control apc ON anr.project_id = apc.id \n" +
                "\tAND anr.company_id = apc.company_id\n" +
                "\tLEFT JOIN auction_project_file apf ON apf.project_id = apc.id \n" +
                "\tAND apf.company_id = apc.company_id \n" +
                "WHERE\n" +
                "\tapc.result_open_range = 1 \n" +
                "\tAND anr.update_time > ? \n" +
                "\tLIMIT ?,?;";
        doSyncNoticeService(auctionDataSource, countSql, querySql, Collections.singletonList(lastSyncTime), RESULT_NOTICE);
    }

    @Override
    protected void refresh(Map<String, Object> result) {
        super.refresh(result);
        result.put(PROJECT_TYPE, AUCTION_NOTICE_TYPE);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
