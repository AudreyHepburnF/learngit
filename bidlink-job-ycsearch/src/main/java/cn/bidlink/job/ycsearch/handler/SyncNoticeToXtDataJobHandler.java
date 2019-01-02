package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.Objects;
import java.util.Properties;

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
        syncPurchaseNoticeData();
        logger.info("同步悦采采购公告结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseNoticeData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.notice",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE)));
//        Timestamp lastSyncTime = new Timestamp(0);
        Timestamp lastSyncStartTime = new Timestamp(new DateTime(new DateTime().getYear(), 1, 1, 0, 0, 0).getMillis());
        if (Objects.equals(SyncTimeUtil.GMT_TIME, lastSyncTime)) {
            lastSyncTime = lastSyncStartTime;
        }
        logger.info("同步悦采采购公告 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "/n" +
                ", syncTime" + SyncTimeUtil.currentDateToString());
        syncYcNoticeService(lastSyncTime);
        // 删除操作状态为-1的公告数据
        deleteRebuildProjectNoticeService();
    }

    private void deleteRebuildProjectNoticeService() {
        Properties properties = elasticClient.getProperties();
        DeleteByQueryRequestBuilder deleteByQueryRequestBuilder = new DeleteByQueryRequestBuilder(elasticClient.getTransportClient(), DeleteByQueryAction.INSTANCE)
                .setIndices(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.notice"))
                .setQuery(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                        .must(QueryBuilders.termQuery(OPT_STATUS, -1))
                );
        deleteByQueryRequestBuilder.execute().actionGet();
    }

    private void syncYcNoticeService(Timestamp lastSyncTime) {
        logger.info("1.开始同步悦采公告数据");
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\topenrangetype = 1 and purchasemodel in (1,7,8) and update_time > ?";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\toriginprojectid AS projectId,\n" +
                "\ttitle AS projectName,\n" +
                "\tbidcode AS projectCode,\n" +
                "\tenddate AS quoteStopTime,\n" +
                "\tpubdate AS publishNoticeTime,\n" +
                "\tprojectintroduction AS projectInfo,\n" +
                "\tlinkman AS linkMan,\n" +
                "\tmobile AS linkTel,\n" +
                "\tif(mobile,1,0) as isShowTel,\n" +
                "\tif(tel,1,0) as isShowMobile,\n" +
                "\ttel AS linkPhone,\n" +
                "\tcompanyid AS companyId,\n" +
                "\tbegindate AS createTime,\n" +
                "\tpurchasemodel AS projectType,\n" +
                "\tinfoType AS noticeType,\n" +
                "\tcontent AS content,\n" +
                "\toptstatus AS optStatus,\n" +
                "\tcompanyname AS companyName \n" +
                "FROM\n" +
                "\t`sync_bulletin` \n" +
                "WHERE\n" +
                "\t openrangetype = 1 and purchasemodel in (1,7,8)  and update_time > ? limit ?,?";
        doSyncNoticeService(ycDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
        logger.info("2.结束同步悦采公告数据");
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
