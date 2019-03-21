//package cn.bidlink.job.business.handler;
//
//import cn.bidlink.job.common.constant.BusinessConstant;
//import cn.bidlink.job.common.es.ElasticClient;
//import cn.bidlink.job.common.utils.DBUtil;
//import cn.bidlink.job.common.utils.ElasticClientUtil;
//import cn.bidlink.job.common.utils.SyncTimeUtil;
//import com.xxl.job.core.biz.model.ReturnT;
//import com.xxl.job.core.handler.annotation.JobHander;
//import org.elasticsearch.action.bulk.BulkRequestBuilder;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.joda.time.DateTime;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Qualifier;
//import org.springframework.stereotype.Service;
//import org.springframework.util.CollectionUtils;
//
//import javax.sql.DataSource;
//import java.math.BigDecimal;
//import java.sql.Timestamp;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
// * @version Ver 1.0
// * @description:同步采购商平台成交项目数据
// * @Date 2018/6/8
// */
//@Service
//@JobHander("syncDealProjectDataJobHandler")
//public class SyncDealProjectDataJobHandler extends JobHandler /*implements InitializingBean*/ {
//
//    private Logger logger = LoggerFactory.getLogger(SyncDealProjectDataJobHandler.class);
//    @Autowired
//    private ElasticClient elasticClient;
//
//    @Autowired
//    @Qualifier("purchaseDataSource")
//    private DataSource purchaseDataSource;
//
//    @Autowired
//    @Qualifier("tenderDataSource")
//    private DataSource tenderDataSource;
//
//    private String  ID                    = "id";
//    private String  COMPANY_ID            = "companyId";
//    private String  TOTAL_PRICE           = "totalPrice";
//    private String  LONG_TOTAL_PRICE      = "longTotalPrice";
//    private String  PROJECT_TYPE          = "projectType";
//    private Integer PURCHASE_PROJECT_TYPE = 2;
//    private Integer BID_PROJECT_TYPE      = 1;
//
//    @Override
//    public ReturnT<String> execute(String... strings) throws Exception {
//        SyncTimeUtil.setCurrentDate();
//        logger.info("开始同步协同平台成交项目数据");
//        syncDealProjectDataService();
//        logger.info("结束同步协同平台成交项目数据");
//        return ReturnT.SUCCESS;
//    }
//
//    private void syncDealProjectDataService() {
//        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.project_index", "cluster.type.project", null);
//        logger.info("协同平台成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
//                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
//        doSyncPurchaseDealProjectService(lastSyncTime);
//        doSyncBidDealProjectService(lastSyncTime);
//    }
//
//    private void doSyncBidDealProjectService(Timestamp lastSyncTime) {
//        String queryCountSql = "SELECT\n" +
//                "\tcount( DISTINCT ( bsp.id ) ) \n" +
//                "FROM\n" +
//                "\tbid_supplier bs\n" +
//                "\tLEFT JOIN bid_sub_project bsp ON bsp.project_id = bs.project_id \n" +
//                "\tAND bsp.id = bs.sub_project_id \n" +
//                "\tAND bsp.company_id = bs.company_id \n" +
//                "WHERE\n" +
//                "\tbsp.project_status IN ( 2, 3 ) \n" +
//                "\tAND bs.win_bid_status = 1 \n" +
//                "\tAND bsp.update_time > ?";
//        String querySql = "SELECT\n" +
//                "\tbsp.id,\n" +
//                "\tbsp.project_name AS name,\n" +
//                "\tbsp.company_id AS companyId,\n" +
//                "\tsum( bs.bid_total_price ) AS totalPrice, \n" +
//                "\tbsp.create_time AS createTime \n" +
//                "FROM\n" +
//                "\tbid_supplier bs\n" +
//                "\tLEFT JOIN bid_sub_project bsp ON bsp.project_id = bs.project_id \n" +
//                "\tAND bsp.id = bs.sub_project_id \n" +
//                "\tAND bsp.company_id = bs.company_id \n" +
//                "WHERE\n" +
//                "\tbsp.project_status IN ( 2, 3 ) \n" +
//                "\tAND bs.win_bid_status = 1 \n" +
//                "\tAND bsp.update_time > ? \n" +
//                "GROUP BY\n" +
//                "\tbsp.id \n" +
//                "\tLIMIT ?,?";
//        List<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
//        doSyncDealProjectService(queryCountSql, querySql, tenderDataSource, params, BID_PROJECT_TYPE);
//    }
//
//    private void doSyncPurchaseDealProjectService(Timestamp lastSyncTime) {
//        String queryCountSql = "SELECT\n" +
//                "\tcount( 1 ) \n" +
//                "FROM\n" +
//                "\tpurchase_project pp\n" +
//                "\tINNER JOIN purchase_project_ext ppe ON pp.id = ppe.id \n" +
//                "\tAND pp.process_status IN ( 31, 40 )\n" +
//                "\tAND pp.update_time >?";
//        String querySql = "SELECT\n" +
//                "\tpp.id,\n" +
//                "\tppe.deal_total_price AS totalPrice,\n" +
//                "\tpp.company_id AS companyId,\n" +
//                "\tpp.NAME ,\n" +
//                "\tpp.create_time AS createTime \n" +
//                "FROM\n" +
//                "\tpurchase_project pp\n" +
//                "\tINNER JOIN purchase_project_ext ppe ON pp.id = ppe.id \n" +
//                "\tAND pp.process_status IN ( 31, 40 )\n" +
//                "\tAND pp.update_time > ?\n" +
//                "\tlimit ?,?";
//        List<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
//        doSyncDealProjectService(queryCountSql, querySql, purchaseDataSource, params, PURCHASE_PROJECT_TYPE);
//    }
//
//    private void doSyncDealProjectService(String queryCountSql, String querySql, DataSource dataSource, List<Object> params, Integer projectType) {
//        long count = DBUtil.count(dataSource, queryCountSql, params);
//        logger.info("执行countSql:{}, params:{}, 共{}条", queryCountSql, params.toString(), count);
//        if (count > 0) {
//            for (long i = 0; i < count; i += pageSize) {
//                List<Object> useToParams = appendToParams(params, i);
//                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, useToParams);
//                logger.info("执行querySql:{}, params:{},共{}条", querySql, useToParams.toString(), mapList.size());
//                refresh(mapList, projectType);
//                batchInsert(mapList);
//            }
//        }
//    }
//
//    private void batchInsert(List<Map<String, Object>> mapList) {
////        System.out.println(mapList);
//        if (!CollectionUtils.isEmpty(mapList)) {
//            BulkRequestBuilder bulkRequestBuilder = elasticClient.getTransportClient().prepareBulk();
//            for (Map<String, Object> projectInfo : mapList) {
//                bulkRequestBuilder.add(elasticClient.getTransportClient().prepareIndex(
//                        elasticClient.getProperties().getProperty("cluster.project_index"),
//                        elasticClient.getProperties().getProperty("cluster.type.project"),
//                        String.valueOf(projectInfo.get(ID)))
//                        .setSource(SyncTimeUtil.handlerDate(projectInfo))
//                );
//            }
//            BulkResponse response = bulkRequestBuilder.execute().actionGet();
//            if (response.hasFailures()) {
//                logger.error(response.buildFailureMessage());
//            }
//        }
//    }
//
//    private void refresh(List<Map<String, Object>> mapList, Integer projectType) {
//        for (Map<String, Object> map : mapList) {
//            map.put(ID, String.valueOf(map.get(ID)));
//            map.put(COMPANY_ID, String.valueOf(map.get(COMPANY_ID)));
//            BigDecimal bigDecimal = (BigDecimal) map.get(TOTAL_PRICE);
//            map.put(TOTAL_PRICE, bigDecimal == null ? "0" : bigDecimal.toString());
//            map.put(LONG_TOTAL_PRICE, bigDecimal == null ? 0 : bigDecimal.longValue());
//            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
//            map.put(PROJECT_TYPE, projectType);
//            //添加平台来源
//            map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
//        }
//    }
//
////    @Override
////    public void afterPropertiesSet() throws Exception {
////        execute();
////    }
//}
