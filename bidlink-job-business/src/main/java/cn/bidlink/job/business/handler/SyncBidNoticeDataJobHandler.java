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
 * @description:新平台招标公告同步
 * @Date 2018/3/15
 */
@Service
@JobHander("syncBidNoticeDataJobHandler")
public class SyncBidNoticeDataJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncBidNoticeDataJobHandler.class);
    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("tenderDataSource")
    protected DataSource tenderDataSource;

    @Value(value = "${pageSize}")
    private Integer pageSize;

    private String ID                 = "id";
    private String COMPANY_ID         = "companyId";
    private String PROJECT_ID         = "projectId";
    private String SYNC_TIME          = "syncTime";
    private String COMPANY_NAME_ALIAS = "companyNameAlias";
    private String COMPANY_NAME       = "companyName";
    private String PROJECT_NAME_ALIAS = "projectNameAlias";
    private String PROJECT_NAME       = "projectName";
    private String NOTICE_TYPE        = "noticeType";


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步新平台招标公告开始");
        syncBidNoticeData();
        logger.info("同步新平台招标公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncBidNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.bid_notice", null);
        logger.info("同步新平台招标公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" +
                ", syncTime" + new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncBidNoticeService(lastSyncTime);
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
                "\tlink_mail AS linkMail\n" +
                "FROM\n" +
                "\t`bid_decided_notice` \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND update_time >?\n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncBidNoticeService(countSql, querySql, params, 2);
    }

    private void syncInsertBidDecidedNotice(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_decided_notice \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
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
                "\tlink_mail AS linkMail\n" +
                "FROM\n" +
                "\t`bid_decided_notice` \n" +
                "WHERE\n" +
                "\tis_publish = 1 \n" +
                "\tAND create_time >?\n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncBidNoticeService(countSql, querySql, params, 2);
    }

    private void syncBidNoticeService(Timestamp lastSyncTime) {
        logger.info("同步协同平台招标公告开始");
        syncInsertBidNoticeService(lastSyncTime);
        syncUpdateBidNoticeService(lastSyncTime);
        logger.info("同步协同平台招标公告结束");
    }

    private void syncInsertBidNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_notice \n" +
                "WHERE\n" +
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
                "\tfile_gain_address AS fileGainAddress \n" +
                "FROM\n" +
                "\tbid_notice \n" +
                "WHERE\n" +
                "\tcreate_time >? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncBidNoticeService(countSql, querySql, params, 1);
    }

    private void syncUpdateBidNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\tbid_notice \n" +
                "WHERE\n" +
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
                "\tfile_gain_address AS fileGainAddress \n" +
                "FROM\n" +
                "\tbid_notice \n" +
                "WHERE\n" +
                "\tupdate_time >? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncBidNoticeService(countSql, querySql, params, 1);
    }

    private void doSyncBidNoticeService(String countSql, String querySql, ArrayList<Object> params, Integer noticeType) {
        long count = DBUtil.count(tenderDataSource, countSql, params);
        logger.debug("执行countSql: {}, params: {}, 共{}条", countSql, params, count);
        for (long i = 0; i < count; i = i + pageSize) {
            // 添加分页
            ArrayList<Object> paramsToUse = paramsToUse(params, i, pageSize);
            List<Map<String, Object>> mapList = DBUtil.query(tenderDataSource, querySql, paramsToUse);
            logger.debug("执行querySql: {}, params: {}, 共{}条", querySql, paramsToUse, mapList.size());
            for (Map<String, Object> result : mapList) {
                result.put(NOTICE_TYPE, noticeType);
                refresh(result);
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
                                elasticClient.getProperties().getProperty("cluster.type.bid_notice"),
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

    private void refresh(Map<String, Object> result) {
        // 处理 id projectId companyId为String类型
        result.put(ID, String.valueOf(result.get(ID)));
        result.put(COMPANY_ID, String.valueOf(result.get(COMPANY_ID)));
        result.put(PROJECT_ID, String.valueOf(result.get(PROJECT_ID)));
        result.put(PROJECT_NAME_ALIAS, result.get(PROJECT_NAME));
        result.put(COMPANY_NAME_ALIAS, result.get(COMPANY_NAME));

        // 添加同步时间字段
        result.put(SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    private ArrayList<Object> paramsToUse(ArrayList<Object> params, long i, Integer pageSize) {
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
