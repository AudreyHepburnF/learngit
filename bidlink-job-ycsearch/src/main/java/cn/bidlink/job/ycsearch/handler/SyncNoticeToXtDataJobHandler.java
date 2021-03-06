package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Objects;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步悦采采购公告
 * @Date 2018/3/16
 */
@Service
@JobHander("syncNoticeToXtDataJobHandler")
public class SyncNoticeToXtDataJobHandler extends AbstractSyncYcNoticeDataJobHandler /*implements InitializingBean*/ {

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步悦采采购公告开始");
        //同步采购公告
        syncPurchaseNoticeData();
        //同步招标和竞价的公告
        syncBidAndAuctionNoticeData();
        // 删除操作状态为-1的公告数据
        deleteRebuildProjectNoticeService();
        logger.info("同步悦采采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.notice_index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE,PURCHASE_NOTICE_TYPE)));
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear() - 1, 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
        logger.info("同步悦采采购公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + SyncTimeUtil.currentDateToString());
        syncYcPurchaseNoticeService(lastSyncTime);
    }

    private void syncBidAndAuctionNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.notice_index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(PROJECT_TYPE,BID_NOTICE_TYPE)));
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear() - 1, 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
        logger.info("同步悦采招标和竞价公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + SyncTimeUtil.currentDateToString());
        syncYcNoticeService(lastSyncTime);
    }

    private void deleteRebuildProjectNoticeService() {
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                .filter(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(OPT_STATUS, -1)))
                .source(elasticClient.getProperties().getProperty("cluster.notice_index"))
                .get();
        if (CollectionUtils.isEmpty(response.getBulkFailures())) {
            logger.error("删除悦采撤项项目公告数据失败");
        }
    }

    private void syncYcPurchaseNoticeService(Timestamp lastSyncTime) {
        logger.info("1.开始同步悦采采购公告数据");
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`sync_bulletin` s \n" +
                "LEFT JOIN bmpfjz_project_ext e ON s.originprojectid = e.id\n" +
                "WHERE\n" +
                "\ts.openrangetype = 1\n " +
                "AND s.purchasemodel =8 \n" +
                "AND ((s.infotype <> '2903' and e.bid_result_show_type=1) OR (s.infotype='2903' and e.purchase_result_show_type=1)) \n" +
                "AND s.update_time > ?";
        String querySql = "SELECT\n" +
                "\ts.id,\n" +
                "\ts.originprojectid AS projectId,\n" +
                "\ts.title AS projectName,\n" +
                "\ts.bidcode AS projectCode,\n" +
                "\ts.enddate AS quoteStopTime,\n" +
                "\ts.pubdate AS publishNoticeTime,\n" +
                "\ts.projectintroduction AS projectInfo,\n" +
                "\ts.linkman AS linkMan,\n" +
                "\ts.email AS linkMail,\n" +
                "\ts.mobile AS linkTel,\n" +
                "\tIF (s.mobile, 1, 0) AS isShowTel,\n" +
                "\tIF (s.tel, 1, 0) AS isShowMobile,\n" +
                "\ts.tel AS linkPhone,\n" +
                "\ts.companyid AS companyId,\n" +
                "\ts.pubdate AS createTime,\n" +
                "\ts.purchasemodel AS projectType,\n" +
                "\ts.infoType AS noticeType,\n" +
                "\ts.content AS content,\n" +
                "\ts.optstatus AS optStatus,\n" +
                "\ts.companyname AS companyName,\n" +
                "\te.purchase_result_show_type AS resultOpenRange,\n" +
                "\tCASE e.amount_show_type WHEN 4 THEN 1 WHEN 1 THEN 1 ELSE 0 END AS priceOpenRange\n" +
                "FROM\n" +
                "\t`sync_bulletin` s\n" +
                "LEFT JOIN bmpfjz_project_ext e ON s.originprojectid = e.id\n" +
                "WHERE\n" +
                "\ts.openrangetype = 1\n" +
                "AND s.purchasemodel = 8\n" +
                "AND ((s.infotype <> '2903' and e.bid_result_show_type=1) OR (s.infotype='2903' and e.purchase_result_show_type=1)) \n" +
                "AND s.update_time > ?\n" +
                "LIMIT ?,?";
        doSyncNoticeService(ycDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
        logger.info("2.结束同步悦采采购公告数据");
    }

    private void syncYcNoticeService(Timestamp lastSyncTime) {
        logger.info("1.开始同步悦采招标和竞价公告数据");
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\topenrangetype = 1 and purchasemodel in (1,7) and update_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\toriginprojectid AS projectId,\n" +
                "\ttitle AS projectName,\n" +
                "\tbidcode AS projectCode,\n" +
                "\tenddate AS quoteStopTime,\n" +
                "\tpubdate AS publishNoticeTime,\n" +
                "\tprojectintroduction AS projectInfo,\n" +
                "\tlinkman AS linkMan,\n" +
                "\temail AS linkMail,\n" +
                "\tmobile AS linkTel,\n" +
                "\tif(mobile,1,0) as isShowTel,\n" +
                "\tif(tel,1,0) as isShowMobile,\n" +
                "\ttel AS linkPhone,\n" +
                "\tcompanyid AS companyId,\n" +
                "\tpubdate AS createTime,\n" +
                "\tpurchasemodel AS projectType,\n" +
                "\tinfoType AS noticeType,\n" +
                "\tcontent AS content,\n" +
                "\toptstatus AS optStatus,\n" +
                "\tcompanyname AS companyName \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\t openrangetype = 1 and purchasemodel in (1,7)  and update_time > ? limit ?,?";
        doSyncNoticeService(ycDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
        logger.info("2.结束同步悦采招标和竞价公告数据");
    }

/*    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
