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
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.*;

@Service
@JobHander("syncDealSupplierProjectToXtDataJobHandler")
public class SyncDealSupplierProjectToXtDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncDealSupplierProjectToXtDataJobHandler.class);
    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    private String  ID                        = "id";
    private String  NAME                      = "name";
    private String  COMPANY_ID                = "companyId";
    private String  COMPANY_NAME              = "companyName";
    private String  COMPANY_NAME_NOT_ANALYZED = "companyNameNotAnalyzed";
    private String  SUPPLIER_ID               = "supplierId";
    private String  PROJECT_ID                = "projectId";
    private String  DEAL_TOTAL_PRICE          = "dealTotalPrice";
    private String  DOUBLE_DEAL_TOTAL_PRICE   = "doubleDealTotalPrice";
    private String  PROJECT_TYPE              = "projectType";
    private Integer AUCTION_PROJECT_TYPE      = 3;
    private Integer PURCHASE_PROJECT_TYPE     = 2;
    private Integer BID_PROJECT_TYPE          = 1;

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步悦采平台成交项目数据");
        syncDealSupplierProjectDataService();
        logger.info("结束同步悦采平台成交项目数据");
        return ReturnT.SUCCESS;
    }

    private void syncDealSupplierProjectDataService() {
        //同步采购项目
        doSyncSupplierPurchaseDealProjectService();
        //同步招标项目
        doSyncSupplierBidDealProjectService();
        //同步竞价打包
        Timestamp timestamp = doSyncSupplierAuction1DealProjectService();
        //同步竞价非打包
        doSyncSupplierAuction2DealProjectService(timestamp);
    }

    //同步招标项目
    private void doSyncSupplierPurchaseDealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(PURCHASE_PROJECT_TYPE);
//        Date lastSyncTime = SyncTimeUtil.toStringDate("2016-07-15 11:22:41");
        logger.info("悦采平台供应商采购成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_bid bspb\n" +
                "INNER JOIN bmpfjz_project bp ON bspb.project_id = bp.id\n" +
                "INNER JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id\n" +
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
                "\tbspb.supplier_name supplierNameNotAnalyzed,\n" +
                "\tbspb.deal_total_price dealTotalPrice,\n" +
                "\tbpe.publish_bid_result_time dealTime\n" +
                "FROM\n" +
                "\tbmpfjz_supplier_project_bid bspb\n" +
                "INNER JOIN bmpfjz_project bp ON bspb.project_id = bp.id\n" +
                "INNER JOIN bmpfjz_project_ext bpe ON bspb.project_id = bpe.id\n" +
                "WHERE\n" +
                "\tbspb.supplier_bid_status = 6\n" +
                "AND bpe.publish_bid_result_time > ?\n" +
                "limit ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, PURCHASE_PROJECT_TYPE);
    }

    //同步采购项目
    private void doSyncSupplierBidDealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(BID_PROJECT_TYPE);
        logger.info("悦采平台供应商招标成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        String queryCountSql = "SELECT\n" +
                "\tCOUNT(*)\n" +
                "FROM\n" +
                "\tbid b\n" +
                "\tLEFT JOIN proj_inter_project pip ON b.PROJECT_ID = pip.ID\n" +
                "\tLEFT JOIN bid_decided bd ON b.PROJECT_ID = bd.PROJECT_ID\n" +
                "WHERE\n" +
                "\tb.IS_BID_SUCCESS = 1\n" +
                "\tAND bd.UPDATE_DATE > ?\n" +
                "\tAND pip.PROJECT_STATUS IN (9, 11)\n" +
                "\tAND NOT EXISTS (\n" +
                "\t\t\tSELECT\n" +
                "\t\t\t\t1\n" +
                "\t\t\tFROM\n" +
                "\t\t\t\tbid_decided bd1\n" +
                "\t\t\tWHERE\n" +
                "\t\t\t\tbd.ROUND < bd1.ROUND\n" +
                "\t\t\t\tAND bd.PROJECT_ID = bd1.PROJECT_ID\n" +
                "\t)";
        String querySql = "SELECT\n" +
                "\tpip.id projectId,\n" +
                "\tpip.PROJECT_NAME projectName,\n" +
                "\tpip.PROJECT_NAME projectNameNotAnalyzed,\n" +
                "\tpip.PROJECT_NUMBER projectCode,\n" +
                "\tpip.COMPANY_ID companyId,\n" +
                "\tb.BIDER_ID supplierId,\n" +
                "\tb.BIDER_NAME supplierNameNotAnalyzed,\n" +
                "\tb.BIDER_PRICE_UNE dealTotalPrice,\n" +
                "\tbd.UPDATE_DATE dealTime\n" +
                "FROM\n" +
                "\tbid b\n" +
                "\tLEFT JOIN proj_inter_project pip ON b.PROJECT_ID = pip.ID\n" +
                "\tLEFT JOIN bid_decided bd ON b.PROJECT_ID = bd.PROJECT_ID\n" +
                "WHERE\n" +
                "\tb.IS_BID_SUCCESS = 1\n" +
                "\tAND bd.UPDATE_DATE > ?\n" +
                "\tAND pip.PROJECT_STATUS IN (9, 11)\n" +
                "\tAND NOT EXISTS (\n" +
                "\t\t\tSELECT\n" +
                "\t\t\t\t1\n" +
                "\t\t\tFROM\n" +
                "\t\t\t\tbid_decided bd1\n" +
                "\t\t\tWHERE\n" +
                "\t\t\t\tbd.ROUND < bd1.ROUND\n" +
                "\t\t\t\tAND bd.PROJECT_ID = bd1.PROJECT_ID\n" +
                "\t) LIMIT ?,?";
        List<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, BID_PROJECT_TYPE);
    }

    //同步竞价打包
    private Timestamp doSyncSupplierAuction1DealProjectService() {
        Timestamp lastSyncTime = getLastSyncTime(AUCTION_PROJECT_TYPE);
        logger.info("悦采平台供应商竞价打包成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
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
                "\tabs.supplier_name supplierNameNotAnalyzed,\n" +
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
        doSyncDealProjectService(queryCountSql, querySql, ycDataSource, params, AUCTION_PROJECT_TYPE);
        return lastSyncTime;
    }

    //同步竞价非打包
    private void doSyncSupplierAuction2DealProjectService(Timestamp lastSyncTime) {
        logger.info("悦采平台供应商竞价非打包成交项目数据lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n" + ",syncTime:" +
                new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
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
                "\t\t\tAND ap.publish_result_time > ?\n" +
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
                "\tabs.supplier_name supplierNameNotAnalyzed,\n" +
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
        Map<Long, String> companyMap = null;
        if (projectType.equals(AUCTION_PROJECT_TYPE) || projectType.equals(BID_PROJECT_TYPE)) {
            String companyIds = getCompanyIds(mapList);
            companyMap = getCompanyMap(companyIds);
        }
        for (Map<String, Object> map : mapList) {
            map.put(ID, generateSupplierProjectId(map));
            if (companyMap != null) {
                String companyName = companyMap.get((Long) map.get(COMPANY_ID));
                map.put(COMPANY_NAME, companyName);
                map.put(COMPANY_NAME_NOT_ANALYZED, companyName);
            }
            map.put(COMPANY_ID, String.valueOf(map.get(COMPANY_ID)));
            map.put(SUPPLIER_ID, String.valueOf(map.get(SUPPLIER_ID)));
            map.put(PROJECT_ID, String.valueOf(map.get(PROJECT_ID)));
            BigDecimal bigDecimal = (BigDecimal) map.get(DEAL_TOTAL_PRICE);
            map.put(DEAL_TOTAL_PRICE, bigDecimal == null ? "0" : bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString());
            map.put(DOUBLE_DEAL_TOTAL_PRICE, bigDecimal == null ? 0 : bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
            map.put(PROJECT_TYPE, projectType);
            //添加平台来源
            map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
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
        return DigestUtils.md5DigestAsHex((supplierId + "_" + projectId + "_" + BusinessConstant.YUECAI_SOURCE).getBytes());
    }

    private String getCompanyIds(List<Map<String, Object>> mapList) {
        Set<Long> companySet = new HashSet<>();
        for (Map<String, Object> map : mapList) {
            companySet.add((Long) map.get(COMPANY_ID));
        }
        return StringUtils.collectionToCommaDelimitedString(companySet);
    }

    private Map<Long, String> getCompanyMap(String companyIds) {
        String querySqlTemplate = "SELECT id,name from t_reg_company where ID in (%s)";
        String querySql = String.format(querySqlTemplate, companyIds);
        List<Map<String, Object>> mapList = DBUtil.query(uniregDataSource, querySql, null);
        Map<Long, String> map = new HashMap<>();
        for (Map<String, Object> company : mapList) {
            map.put((Long) company.get(ID), (String) company.get(NAME));
        }
        return map;

    }

    private Timestamp getLastSyncTime(Integer projectType) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.must(QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
                .must(QueryBuilders.termQuery(PROJECT_TYPE, projectType));
        return ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.deal_supplier_project", queryBuilder);
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
