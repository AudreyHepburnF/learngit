package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步招募所有信息到招募索引中
 * @Date 2018/12/5
 */
@Service
@JobHander(value = "syncRecruitXtDataJobHandler")
public class SyncRecruitXtDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncRecruitXtDataJobHandler.class);

    @Autowired
    @Qualifier(value = "recruitDataSource")
    private DataSource recruitDataSource;

    @Autowired
    private ElasticClient elasticClient;

    private String PURCHASE_ID = "purchaseId";
    private String ID          = "id";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("1.开始同步招募详情信息数据");
        syncRecruitData();
        logger.info("2.结束同步招募详情信息数据");
        return ReturnT.SUCCESS;
    }

    private void syncRecruitData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.recruit_index", "cluster.type.recruit",
               null);
//        Timestamp lastSyncTime = new Timestamp(0);
        logger.info("1.1 同步招募信息lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n" + ",syncTime:" + SyncTimeUtil.currentDateToString());
        syncRecruitDataService(lastSyncTime);
    }

    private void syncRecruitDataService(Timestamp lastSyncTime) {
        String insertCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\tDEL_FLAG = 1  \n" +
                "\tAND CREATE_TIME > ?";

        String insertQuerySql = "SELECT\n" +
                "\tr.id AS id,\n" +
                "\tr.id AS projectId,\n" +
                "\tr.TITLE AS projectName,\n" +
                "\tr.content AS content,\n" +
                "\tr.sdate AS startDate,\n" +
                "\tr.edate AS endDate,\n" +
                "\tr.AREA_NAME AS areaName,\n" +
                "\tr.AREA_LIMITE AS areaLimit,\n" +
                "\tr.QUALIFICATION_NAME AS qualificationName,\n" +
                "\tr.PURCHASER_ID AS purchaseId,\n" +
//                "\tr.PURCHASER AS purchaseName,\n" +
                "\tr.status,\n" +
                "\tr.endless,\n" +
                "\tr.CREATE_TIME AS createTime,\n" +
                "\tr.UPDATE_TIME AS updateTime,\n" +
                "\tr.IF_APPROVE AS ifApprove,\n" +
                "\tr.IF_RESTRICTION AS ifRestriction,\n" +
                "\tr.PART_CATEGORY_NAME AS partCategoryName,\n" +
                "\trf.FILE_NAME AS fileName,\n" +
                "\trf.FILE_PATH AS md5,\n" +
                "\tIFNULL(r.PLATFORM_SOURCE,2) AS platformSource\n" +
                "FROM\n" +
                "\t`recruit` r\n" +
                "LEFT JOIN `recruit_files` rf ON (rf.RECRUIT_ID=r.ID AND rf.DEL_FLAG=1)\n" +
                "WHERE\n" +
                "\tr.DEL_FLAG = 1 and r.create_time > ?\n" +
                "\t limit ?,?";
        doSncRecruitDataService(recruitDataSource, insertCountSql, insertQuerySql, Collections.singletonList(lastSyncTime));

        String updateCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`recruit` \n" +
                "WHERE\n" +
                "\tDEL_FLAG = 1 \n" +
                "\tAND update_time > ?";

        String updateQuerySql = "SELECT\n" +
                "\tr.id AS id,\n" +
                "\tr.id AS projectId,\n" +
                "\tr.TITLE AS projectName,\n" +
                "\tr.content AS content,\n" +
                "\tr.sdate AS startDate,\n" +
                "\tr.edate AS endDate,\n" +
                "\tr.AREA_LIMITE AS areaLimit,\n" +
                "\tr.AREA_NAME AS areaName,\n" +
                "\tr.QUALIFICATION_NAME AS qualificationName,\n" +
                "\tr.PURCHASER_ID AS purchaseId,\n" +
//                "\tr.PURCHASER AS purchaseName,\n" +
                "\tr.status,\n" +
                "\tr.endless,\n" +
                "\tr.CREATE_TIME AS createTime,\n" +
                "\tr.UPDATE_TIME AS updateTime,\n" +
                "\tr.IF_APPROVE AS ifApprove,\n" +
                "\tr.IF_RESTRICTION AS ifRestriction,\n" +
                "\tr.PART_CATEGORY_NAME AS partCategoryName,\n" +
                "\trf.FILE_NAME AS fileName,\n" +
                "\trf.FILE_PATH AS md5,\n" +
                "\tIFNULL(r.PLATFORM_SOURCE,2) AS platformSource\n" +
                "FROM\n" +
                "\t`recruit` r\n" +
                "LEFT JOIN `recruit_files` rf ON (rf.RECRUIT_ID=r.ID AND rf.DEL_FLAG=1)\n" +
                "WHERE\n" +
                "\tr.DEL_FLAG = 1 and r.update_time > ?\n" +
                "\t limit ?,?";
        doSncRecruitDataService(recruitDataSource, updateCountSql, updateQuerySql, Collections.singletonList(lastSyncTime));
    }

    private void doSncRecruitDataService(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; ) {
                List<Object> paramsToUse = appendToParams(params, i);
                // 查出符合条件的商机
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, paramsToUse, mapList.size());
                for (Map<String, Object> map : mapList) {
                    refresh(map);
                }
                // 处理商机的状态
                batchExecute(mapList);
                i += pageSize;
            }
        }

    }

    private void refresh(Map<String, Object> map) {
        map.put(PURCHASE_ID, String.valueOf(map.get(PURCHASE_ID)));
        map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
        //招募用的同一张表取里面的字段 主表有该字段
//        map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);

    }

    protected void batchExecute(List<Map<String, Object>> resultsToUpdate) {
//        System.out.println("size : " + resultsToUpdate.size());
//        for (Map<String, Object> map : resultsToUpdate) {
//            System.out.println(map);
//        }
        if (!CollectionUtils.isEmpty(resultsToUpdate)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> result : resultsToUpdate) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.recruit_index"),
                                elasticClient.getProperties().getProperty("cluster.type.recruit"),
                                String.valueOf(result.get(ID)))
                        .setSource(SyncTimeUtil.handlerDate(result)));
            }

            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
