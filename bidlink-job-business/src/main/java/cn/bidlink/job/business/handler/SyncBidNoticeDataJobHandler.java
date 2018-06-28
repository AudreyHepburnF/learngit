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
 * @description:新平台招标公告同步
 * @Date 2018/3/15
 */
@Service
@JobHander("syncBidNoticeDataJobHandler")
public class SyncBidNoticeDataJobHandler extends AbstractSyncNoticeDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步新平台招标公告开始");
        syncBidNoticeData();
        logger.info("同步新平台招标公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncBidNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.notice",
                QueryBuilders.termQuery(PROJECT_TYPE, BID_NOTICE_TYPE));
//        Timestamp lastSyncTime = SyncTimeUtil.GMT_TIME;
        logger.info("同步新平台招标公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" +
                ", syncTime" + new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncUnderWayBidNoticeService(lastSyncTime);
        syncBidDecidedNoticeService(lastSyncTime);
    }

    private void syncBidDecidedNoticeService(Timestamp lastSyncTime) {
        logger.info("同步协同平台中标公告开始");
        syncInsertBidDecidedNotice(lastSyncTime);
        syncUpdateBidDecidedNotice(lastSyncTime);
        logger.info("同步协同平台中标公告结束");
    }

    private void syncUpdateBidDecidedNotice(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_decided_notice \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND approve_status = 2 \n" +
                "\tAND update_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tsub_project_id AS subProjectId,\n" +
                "\tproject_name AS projectName,\n" +
                "\tproject_code AS projectCode,\n" +
                "\ttender_name AS tenderName,\n" +
                "\tbid_type AS bidType,\n" +
                "\tis_have_wibider AS isHaveWibider,\n" +
                "\tsupplier_name AS supplierNameNotAnalyzed,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_id AS companyId\n" +
                "FROM\n" +
                "\t`bid_decided_notice` \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND approve_status = 2 \n" +
                "\tAND update_time >?\n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(tenderDataSource, countSql, querySql, params, RESULT_NOTICE);
    }

    private void syncInsertBidDecidedNotice(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_decided_notice \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND approve_status= 2 \n" +
                "\tAND create_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tproject_id AS projectId,\n" +
                "\tsub_project_id AS subProjectId,\n" +
                "\tproject_name AS projectName,\n" +
                "\tproject_code AS projectCode,\n" +
                "\ttender_name AS tenderName,\n" +
                "\tbid_type AS bidType,\n" +
                "\tis_have_wibider AS isHaveWibider,\n" +
                "\tsupplier_name AS supplierNameNotAnalyzed,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tcompany_id AS companyId\n" +
                "FROM\n" +
                "\t`bid_decided_notice` \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND approve_status= 2 \n" +
                "\tAND create_time >?\n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(tenderDataSource, countSql, querySql, params, RESULT_NOTICE);
    }

    private void syncUnderWayBidNoticeService(Timestamp lastSyncTime) {
        logger.info("同步协同平台初始公告和变更公告开始");
        syncInsertBidNoticeService(lastSyncTime);
        syncUpdateBidNoticeService(lastSyncTime);
        logger.info("同步协同平台初始公告和变更公告结束");
    }

    private void syncInsertBidNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_notice_history \n" +
                "WHERE\n" +
                "\tapprove_status=2 and \n" +
                "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tid AS id,\n" +
                "\tproject_id AS projectId,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tproject_name AS projectName,\n" +
                "\tproject_code AS projectCode,\n" +
                "\ttender_name AS tenderName,\n" +
                "\tbid_endtime AS bidEndTime,\n" +
                "\tbid_type AS bidType,\n" +
                "\tbid_describe AS bidDescribe,\n" +
                "\tqualification,\n" +
                "\t\tis_doc_free AS docFree,\n" +
                "\tbid_doc_money AS bidDocMoney,\n" +
                "\tbid_bail AS bidBail,\n" +
                "\tis_bail_free AS bailFree,\n" +
                "\tput_file_type AS putFileType,\n" +
                "\tbid_open_time AS bidOpenTime,\n" +
                "\tbid_open_address AS bidOpenAddress,\n" +
                "\tremarks AS remarks,\n" +
                "\topen_type AS openType,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_tel_is_show AS linkTelIsShow,\n" +
                "\tlink_phone_is_show AS linkPhoneIsShow,\n" +
                "\tunit AS unit,\n" +
                "\tbid_doc_start_time AS bidDocStartTime,\n" +
                "\tbid_doc_over_time AS bidDocOverTime,\n" +
                "\tcompany_name AS companyName,\n" +
                "\tnotice_publish_time AS noticePublishTime,\n" +
                "\tsub_project_id AS subProjectId,\n" +
                "\tgain_file_type AS gainFileType,\n" +
                "\tfile_gain_address AS fileGainAddress, \n" +
                "\tcreate_time AS createTime \n" +
                "FROM\n" +
                "\tbid_notice_history \n" +
                "WHERE\n" +
                "\tapprove_status=2 and \n" +
                "\tcreate_time >? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(tenderDataSource, countSql, querySql, params, SOURCE_NOTICE);
    }

    private void syncUpdateBidNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_notice_history \n" +
                "WHERE\n" +
                "\tapprove_status=2 and \n" +
                "\tupdate_time > ?";
        String querySql = "SELECT\n" +
                "\tid AS id,\n" +
                "\tproject_id AS projectId,\n" +
                "\tcompany_id AS companyId,\n" +
                "\tproject_name AS projectName,\n" +
                "\tproject_code AS projectCode,\n" +
                "\ttender_name AS tenderName,\n" +
                "\tbid_endtime AS bidEndTime,\n" +
                "\tbid_type AS bidType,\n" +
                "\tbid_describe AS bidDescribe,\n" +
                "\tqualification,\n" +
                "\t\tis_doc_free AS isDocFree,\n" +
                "\tbid_doc_money AS bidDocMoney,\n" +
                "\tis_bail_free AS isBailFree,\n" +
                "\tbid_bail AS bidBail,\n" +
                "\tput_file_type AS putFileType,\n" +
                "\tbid_open_time AS bidOpenTime,\n" +
                "\tbid_open_address AS bidOpenAddress,\n" +
                "\tremarks AS remarks,\n" +
                "\topen_type AS openType,\n" +
                "\tlink_man AS linkMan,\n" +
                "\tlink_mail AS linkMail,\n" +
                "\tlink_phone AS linkPhone,\n" +
                "\tlink_tel AS linkTel,\n" +
                "\tlink_tel_is_show AS linkTelIsShow,\n" +
                "\tlink_phone_is_show AS linkPhoneIsShow,\n" +
                "\tunit AS unit,\n" +
                "\tbid_doc_start_time AS bidDocStartTime,\n" +
                "\tbid_doc_over_time AS bidDocOverTime,\n" +
                "\tcompany_name AS companyName,\n" +
                "\tnotice_publish_time AS noticePublishTime,\n" +
                "\tsub_project_id AS subProjectId,\n" +
                "\tgain_file_type AS gainFileType,\n" +
                "\tfile_gain_address AS fileGainAddress, \n" +
                "\tcreate_time AS createTime \n" +
                "FROM\n" +
                "\tbid_notice_history \n" +
                "WHERE\n" +
                "\tapprove_status=2 and \n" +
                "\tupdate_time >? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncNoticeService(tenderDataSource, countSql, querySql, params, SOURCE_NOTICE);
    }

    @Override
    protected void refresh(Map<String, Object> result) {
        super.refresh(result);
        // 公告类型为招标公告
        result.put(PROJECT_TYPE, BID_NOTICE_TYPE);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
