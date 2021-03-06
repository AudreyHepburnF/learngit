package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@JobHander("syncDealSupplierProjectDataJobHandler")
public class SyncDealSupplierProjectDataJobHandler extends JobHandler /*implements InitializingBean*/{

    private Logger logger = LoggerFactory.getLogger(SyncDealSupplierProjectDataJobHandler.class);
    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("purchaseDataSource")
    private DataSource purchaseDataSource;

    @Autowired
    @Qualifier("tenderDataSource")
    private DataSource tenderDataSource;

    @Autowired
    @Qualifier("auctionDataSource")
    private DataSource auctionDataSource;

    private String  ID                      = "id";
    private String  COMPANY_ID              = "companyId";
    private String  SUPPLIER_ID             = "supplierId";
    private String  PROJECT_ID              = "projectId";
    private String  DEAL_TOTAL_PRICE        = "dealTotalPrice";
    private String  DOUBLE_DEAL_TOTAL_PRICE = "doubleDealTotalPrice";
    private String  PROJECT_TYPE            = "projectType";
    private Integer AUCTION_PROJECT_TYPE    = 3;
    private Integer PURCHASE_PROJECT_TYPE   = 2;
    private Integer BID_PROJECT_TYPE        = 1;


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步协同平台成交项目数据");
        syncDealSupplierProjectDataService();
        logger.info("结束同步协同平台成交项目数据");
        return ReturnT.SUCCESS;
    }

    private void syncDealSupplierProjectDataService() {
        //同步采购项目
        doSyncSupplierPurchaseDealProjectService();
        //同步招标项目
        doSyncSupplierBidDealProjectService();
        //同步竞价项目
        doSyncSupplierAuctionDealProjectService();
    }

    private void doSyncSupplierBidDealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(BID_PROJECT_TYPE);
        logger.info("协同平台供应商招标成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tbid_supplier bs\n" +
                "WHERE\n" +
                "\tbs.win_bid_status = 1\n" +
                "AND bs.win_bid_time > ?";
        String querySql = "SELECT\n" +
                "\tbs.sub_project_id projectId,\n" +
                "\tbs.project_name projectName,\n" +
                "\tbs.project_name projectNameNotAnalyzed,\n" +
                "\tbs.project_code projectCode,\n" +
                "\tbs.company_id companyId,\n" +
                "\tbs.company_name companyName,\n" +
                "\tbs.company_name companyNameNotAnalyzed,\n" +
                "\tbs.supplier_id supplierId,\n" +
                "\tbs.supplier_name supplierNameNotAnalyzed,\n" +
                "\tbs.win_bid_time dealTime,\n" +
                "\tbs.win_bid_total_price dealTotalPrice\n" +
                "FROM\n" +
                "\tbid_supplier bs\n" +
                "WHERE\n" +
                "\tbs.win_bid_status = 1\n" +
                "AND bs.win_bid_time > ?\n" +
                "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, tenderDataSource, params, BID_PROJECT_TYPE);
    }

    private void doSyncSupplierPurchaseDealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(PURCHASE_PROJECT_TYPE);
        logger.info("协同平台供应商采购成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tpurchase_supplier_project psp\n" +
                "LEFT JOIN purchase_project pp ON psp.project_id = pp.id\n" +
                "LEFT JOIN purchase_project_ext ppe ON psp.project_id = ppe.id\n" +
                "WHERE\n" +
                "\tpsp.deal_status = 2\n" +
                "AND pp.id IS NOT NULL\n" +
                "AND ppe.publish_result_time > ?";
        String querySql = "SELECT\n" +
                "\tpp.id projectId,\n" +
                "\tpp.`name` projectName,\n" +
                "\tpp.`name` projectNameNotAnalyzed,\n" +
                "\tpp.`code` projectCode,\n" +
                "\tpp.company_id companyId,\n" +
                "\tpp.company_name companyName,\n" +
                "\tpp.company_name companyNameNotAnalyzed,\n" +
                "\tpsp.supplier_id supplierId,\n" +
                "\tpsp.supplier_name supplierNameNotAnalyzed,\n" +
                "\tppe.publish_result_time dealTime,\n" +
                "\tpsp.deal_total_price dealTotalPrice\n" +
                "FROM\n" +
                "\tpurchase_supplier_project psp\n" +
                "LEFT JOIN purchase_project pp ON psp.project_id = pp.id\n" +
                "LEFT JOIN purchase_project_ext ppe ON psp.project_id = ppe.id\n" +
                "WHERE\n" +
                "\tpsp.deal_status = 2\n" +
                "AND pp.id IS NOT NULL\n" +
                "AND ppe.publish_result_time > ?\n" +
                "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, purchaseDataSource, params, PURCHASE_PROJECT_TYPE);
    }

    private void doSyncSupplierAuctionDealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(AUCTION_PROJECT_TYPE);
        logger.info("协同平台供应商竞价成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tauction_supplier_project asp\n" +
                "\tLEFT JOIN auction_project ap ON asp.project_id = ap.id\n" +
                "\tLEFT JOIN auction_project_ext ape ON asp.project_id = ape.id\n" +
                "WHERE\n" +
                "\tasp.deal_status = 3\n" +
                "AND ap.id IS NOT NULL\n" +
                "AND ape.publish_result_time > ?";
        String querySql = "SELECT\n" +
                "\tap.id projectId,\n" +
                "\tap.`name` projectName,\n" +
                "\tap.`name` projectNameNotAnalyzed,\n" +
                "\tap.`code` projectCode,\n" +
                "\tap.company_id companyId,\n" +
                "\tap.company_name companyName,\n" +
                "\tap.company_name companyNameNotAnalyzed,\n" +
                "\tasp.supplier_id supplierId,\n" +
                "\tasp.supplier_name supplierNameNotAnalyzed,\n" +
                "\tape.publish_result_time dealTime,\n" +
                "\tasp.deal_total_price dealTotalPrice\n" +
                "FROM\n" +
                "\tauction_supplier_project asp\n" +
                "\tLEFT JOIN auction_project ap ON asp.project_id = ap.id\n" +
                "\tLEFT JOIN auction_project_ext ape ON asp.project_id = ape.id\n" +
                "WHERE\n" +
                "\tasp.deal_status = 3\n" +
                "AND ap.id IS NOT NULL\n" +
                "AND ape.publish_result_time > ?\n" +
                "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, auctionDataSource, params, AUCTION_PROJECT_TYPE);
    }

    private void doSyncDealProjectService(String queryCountSql, String querySql, DataSource dataSource, List<Object> params, Integer projectType) {
        long count = DBUtil.count(dataSource, queryCountSql, params);
        logger.info("执行countSql:{}, params:{}, 共{}条", queryCountSql, params.toString(), count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> useToParams = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, useToParams);
                logger.info("执行querySql:{}, params:{},共{}条", querySql, useToParams.toString(), mapList.size());
                refresh(mapList, projectType);
                batchInsert(mapList);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequestBuilder = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> projectInfo : mapList) {
                bulkRequestBuilder.add(elasticClient.getTransportClient().prepareIndex(
                        elasticClient.getProperties().getProperty("cluster.deal_supplier_project"),
                        elasticClient.getProperties().getProperty("cluster.type.deal_supplier_project"),
                        String.valueOf(projectInfo.get(ID)))
                        .setSource(SyncTimeUtil.handlerDate(projectInfo))
                );
            }
            BulkResponse response = bulkRequestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    private void refresh(List<Map<String, Object>> mapList, Integer projectType) {
        for (Map<String, Object> map : mapList) {
            map.put(ID, generateSupplierProjectId(map));
            map.put(COMPANY_ID, String.valueOf(map.get(COMPANY_ID)));
            map.put(SUPPLIER_ID, String.valueOf(map.get(SUPPLIER_ID)));
            map.put(PROJECT_ID, String.valueOf(map.get(PROJECT_ID)));
            BigDecimal bigDecimal = (BigDecimal) map.get(DEAL_TOTAL_PRICE);
            map.put(DEAL_TOTAL_PRICE, bigDecimal == null ? "0" : bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString());
            map.put(DOUBLE_DEAL_TOTAL_PRICE, bigDecimal == null ? 0 : bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
            map.put(PROJECT_TYPE, projectType);
            //添加平台来源
            map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
        }
    }

    private String generateSupplierProjectId(Map<String, Object> result) {
        Long supplierId = (Long) result.get(SUPPLIER_ID);
        Long projectId = (Long) (result.get(PROJECT_ID));
        if (supplierId == null) {
            throw new RuntimeException("供应商项目ID生成失败，原因：供应商ID为null!");
        }
        if (projectId == null) {
            throw new RuntimeException("供应商项目ID生成失败，原因：projectId为null!");
        }
        return DigestUtils.md5DigestAsHex((supplierId + "_" + projectId + "_" + BusinessConstant.IXIETONG_SOURCE).getBytes());
    }

    private Timestamp getLastSyncTime(Integer projectType) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE))
                .must(QueryBuilders.termQuery(PROJECT_TYPE, projectType));
        return ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.deal_supplier_project", "cluster.type.deal_supplier_project", queryBuilder);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
