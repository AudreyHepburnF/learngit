package cn.bidlink.job.ycsearch.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
@JobHander("syncDealSupplierProjectToXtDataJobHandler")
public class SyncDealSupplierProjectToXtDataJobHandler extends JobHandler {

    private Logger logger = LoggerFactory.getLogger(SyncDealSupplierProjectToXtDataJobHandler.class);
    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    private String  ID                    = "id";
    private String  COMPANY_ID            = "companyId";
    private String  SUPPLIER_ID            = "supplierId";
    private String  PROJECT_ID            = "projectId";
    private String  DEAL_TOTAL_PRICE      = "dealTotalPrice";
    private String  LONG_DEAL_TOTAL_PRICE = "longDealTotalPrice";
    private String  PROJECT_TYPE          = "projectType";
    private Integer AUCTION_PROJECT_TYPE = 3;
    private Integer PURCHASE_PROJECT_TYPE = 2;
    private Integer BID_PROJECT_TYPE      = 1;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步悦采平台成交项目数据");
        syncDealSupplierProjectDataService();
        logger.info("结束同步悦采平台成交项目数据");
        return ReturnT.SUCCESS;
    }

    private void syncDealSupplierProjectDataService() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.deal_supplier_project", null);
        logger.info("悦采平台成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        //同步采购项目
        doSyncSupplierPurchaseDealProjectService(lastSyncTime);
        //同步招标项目
        doSyncSupplierBidDealProjectService(lastSyncTime);
        //同步竞价打包
        doSyncSupplierAuction1DealProjectService(lastSyncTime);
        //同步竞价非打包
        doSyncSupplierAuction2DealProjectService(lastSyncTime);
    }

    //同步招标项目
    private void doSyncSupplierPurchaseDealProjectService(Timestamp lastSyncTime) {
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_bid bspb\n" +
                "LEFT JOIN bmpfjz_project bp ON bspb.project_id = bp.id\n" +
                "LEFT JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id\n" +
                "WHERE\n" +
                "\tbspb.supplier_bid_status = 6\n" +
                "AND bpe.publish_bid_result_time > ?";
        String querySql = "SELECT\n" +
                "\tbp.id projectId,\n" +
                "\tbp.`name` projectName,\n" +
                "\tbp.`name` projectNameNotAnalyzed,\n" +
                "\tbp.`code` projectCode,\n" +
                "\tbp.comp_id companyId,\n" +
                "\tbp.comp_name companyName,\n" +
                "\tbp.comp_name companyNameNotAnalyzed,\n" +
                "\tbspb.supplier_id supplierId,\n" +
                "\tbspb.supplier_name supplierName,\n" +
                "\tbspb.deal_total_price dealTotalPrice,\n" +
                "\tbpe.publish_bid_result_time dealTime\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_bid bspb\n" +
                "LEFT JOIN bmpfjz_project bp ON bspb.project_id = bp.id\n" +
                "LEFT JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id\n" +
                "WHERE\n" +
                "\tbspb.supplier_bid_status = 6\n" +
                "AND bpe.publish_bid_result_time > ?\n" +
                "limit ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, PURCHASE_PROJECT_TYPE);
    }

    //同步采购项目
    private void doSyncSupplierBidDealProjectService(Timestamp lastSyncTime) {
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tauction_bid_supplier abs\n" +
                "\tLEFT JOIN auction_project ap ON abs.project_id = ap.id\n" +
                "WHERE\n" +
                "\tabs.bid_status = 1\n" +
                "AND ap.publish_result_time > ?\n" +
                "AND ap.project_type = 1";
        String querySql = "SELECT\n" +
                "\tap.id projectId,\n" +
                "\tap.project_name projectName,\n" +
                "\tap.project_name projectNameNotAnalyzed,\n" +
                "\tap.project_code projectCode,\n" +
                "\tap.comp_id companyId,\n" +
                "\tabs.supplier_id supplierId,\n" +
                "\tabs.supplier_name supplierName,\n" +
                "\tap.publish_result_time dealTime,\n" +
                "\tIFNULL(abs.final_price,abs.real_price) dealTotalPrice\n" +
                "FROM\n" +
                "\tauction_bid_supplier abs\n" +
                "LEFT JOIN auction_project ap ON abs.project_id = ap.id\n" +
                "WHERE\n" +
                "\tabs.bid_status = 1\n" +
                "AND ap.publish_result_time > ?\n" +
                "AND ap.project_type = 1\n" +
                "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, BID_PROJECT_TYPE);
    }

    //同步竞价打包
    private void doSyncSupplierAuction1DealProjectService(Timestamp lastSyncTime) {
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tap.id projectId,\n" +
                "\t\t\tabs.supplier_id supplierId\n" +
                "\t\tFROM\n" +
                "\t\t\tauction_bid_supplier abs\n" +
                "\t\t\tLEFT JOIN auction_project ap ON abs.project_id = ap.id\n" +
                "\t\t\tLEFT JOIN auction_directory_info adi ON (abs.directory_id=adi.directory_id AND abs.project_id=adi.project_id)\n" +
                "\t\tWHERE\n" +
                "\t\t\tabs.bid_status = 1\n" +
                "\t\t\tAND ap.publish_result_time > 0\n" +
                "\t\t\tAND ap.project_type = 2\n" +
                "\t\t\tAND abs.divide_rate IS NOT NULL\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tabs.project_id,\n" +
                "\t\t\tabs.supplier_id\n" +
                "\t) p";
        String querySql = "SELECT\n" +
                "\tap.id projectId,\n" +
                "\tap.project_name projectName,\n" +
                "\tap.project_name projectNameNotAnalyzed,\n" +
                "\tap.project_code projectCode,\n" +
                "\tap.comp_id companyId,\n" +
                "\tabs.supplier_id supplierId,\n" +
                "\tabs.supplier_name supplierName,\n" +
                "\tap.publish_result_time dealTime,\n" +
                "\tSUM(IFNULL(abs.final_price,abs.real_price)*adi.plan_amount*abs.divide_rate/100) dealTotalPrice\n" +
                "FROM\n" +
                "\tauction_bid_supplier abs\n" +
                "LEFT JOIN auction_project ap ON abs.project_id = ap.id\n" +
                "LEFT JOIN auction_directory_info adi ON (abs.directory_id=adi.directory_id AND abs.project_id=adi.project_id)\n" +
                "WHERE\n" +
                "\tabs.bid_status = 1\n" +
                "AND ap.publish_result_time > ?\n" +
                "AND ap.project_type = 2\n" +
                "AND abs.divide_rate IS NOT NULL\n" +
                "GROUP BY\n" +
                "\tabs.project_id,\n" +
                "\tabs.supplier_id\n" +
                "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, AUCTION_PROJECT_TYPE);
    }

    //同步竞价非打包
    private void doSyncSupplierAuction2DealProjectService(Timestamp lastSyncTime) {
        String queryCountSql = "";
        String querySql = "";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, AUCTION_PROJECT_TYPE);
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
                        elasticClient.getProperties().getProperty("cluster.index"),
                        elasticClient.getProperties().getProperty("cluster.type.deal_supplier_project"),
                        String.valueOf(projectInfo.get(ID)))
                        .setSource(JSON.toJSONString(projectInfo, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {
                                    return propertyValue;
                                }
                            }
                        }))
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
            map.put(DEAL_TOTAL_PRICE, bigDecimal == null ? "0" : bigDecimal.toString());
            map.put(LONG_DEAL_TOTAL_PRICE, bigDecimal == null ? 0 : bigDecimal.longValue());
            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
            map.put(PROJECT_TYPE, projectType);
            //添加平台来源
            map.put(BusinessConstant.PLATFORM_SOURCE_KEY,BusinessConstant.YUECAI_SOURCE);
        }
    }

    private String generateSupplierProjectId(Map<String, Object> result) {
        Long supplierId = (Long) result.get(SUPPLIER_ID);
        Long projectId = (Long)(result.get(PROJECT_ID));
        if (supplierId == null) {
            throw new RuntimeException("供应商项目ID生成失败，原因：供应商ID为null!");
        }
        if (projectId==null) {
            throw new RuntimeException("供应商项目ID生成失败，原因：projectId为null!");
        }
        return DigestUtils.md5DigestAsHex((supplierId + "_" + projectId+"_"+BusinessConstant.YUECAI_SOURCE).getBytes());
    }

}
