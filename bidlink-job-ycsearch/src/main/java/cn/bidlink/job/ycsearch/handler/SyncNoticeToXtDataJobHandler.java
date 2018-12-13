package cn.bidlink.job.ycsearch.handler;

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
//        long startTime = System.currentTimeMillis();
        syncPurchaseNoticeData();
//        long endTime = System.currentTimeMillis();
//        System.out.println("同步时间差:" + (endTime - startTime));
        logger.info("同步悦采采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(PROJECT_TYPE, PURCHASE_NOTICE_TYPE))
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE)));
        logger.info("同步悦采采购公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + SyncTimeUtil.currentDateToString());
        syncPurchaseUnderwayNoticeService(lastSyncTime);
    }

    private void syncPurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        logger.info("同步原始公告和变更公告开始");
        syncInsertPurchaseUnderwayNoticeService(lastSyncTime);
        logger.info("同步原始公告和变更公告结束");
    }

    private void syncInsertPurchaseUnderwayNoticeService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\topenrangetype = 1 and purchasemodel in (7,8)";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tprojectcode AS projectId,\n" +
                "\ttitle AS projectName,\n" +
                "\tbidcode AS projectCode,\n" +
                "\tenddate AS quoteStopTime,\n" +
                "\tpubdate AS publishNoticeTime,\n" +
                "\tprojectintroduction AS projectInfo,\n" +
                "\tlinkman AS linkMan,\n" +
                "\ttel AS linkTel,\n" +
                "\tif(tel,1,0) as isShowTel,\n" +
                "\tif(mobile,1,0) as isShowMobile,\n" +
                "\tmobile AS linkPhone,\n" +
                "\tcompanyid AS companyId,\n" +
                "\tbegindate AS createTime,\n" +
                "\tpurchasemodel AS projectType,\n" +
                "\tinfoType AS noticeType,\n" +
                "\tcompanyname AS companyName \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\t openrangetype = 1 and purchasemodel in (7,8) limit ?,?";
        doSyncNoticeService(ycDataSource, countSql, querySql, Collections.emptyList());
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
