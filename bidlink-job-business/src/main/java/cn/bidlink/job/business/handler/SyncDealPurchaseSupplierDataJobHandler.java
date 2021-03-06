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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步成交供应商
 * @Date 2018/6/28
 */
@Service
@JobHander("syncDealPurchaseSupplierDataJobHandler")
public class SyncDealPurchaseSupplierDataJobHandler extends JobHandler /*implements InitializingBean */{

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("tenderDataSource")
    private DataSource tenderDataSource;

    @Autowired
    @Qualifier("purchaseDataSource")
    private DataSource purchaseDataSource;

    @Autowired
    @Qualifier("auctionDataSource")
    private DataSource auctionDataSource;

    private   Logger logger                = LoggerFactory.getLogger(SyncDealPurchaseSupplierDataJobHandler.class);
    protected int    BIDDING_PROJECT_TYPE  = 1;
    protected int    PURCHASE_PROJECT_TYPE = 2;
    protected int    AUCTION_PROJECT_TYPE  = 3;

    private String ID                         = "id";
    private String COMPANY_ID                 = "companyId";
    private String SUPPLIER_NAME_NOT_ANALYZED = "supplierNameNotAnalyzed";
    private String SUPPLIER_NAME              = "supplierName";
    private String SUPPLIER_ID                = "supplierId";
    private String PROJECT_TYPE               = "projectType";
    private String PROJECT_ID                 = "projectId";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步成交供应商");
        syncDealPurchaseSupplier();
        logger.info("结束同步成交供应商");
        return ReturnT.SUCCESS;
    }

    private void syncDealPurchaseSupplier() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.deal_supplier_index", "cluster.type.deal_supplier", null);
        logger.info("同步成交供应商lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + ",\n" + "syncTime:"
                + new DateTime().toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncDealPurchaseSupplierService(lastSyncTime);
    }

    private void syncDealPurchaseSupplierService(Timestamp lastSyncTime) {
        syncDealPurchaseProjectService(lastSyncTime);
        syncDealBidProjectService(lastSyncTime);
        syncDealAuctionProjectService(lastSyncTime);
    }

    private void syncDealBidProjectService(Timestamp lastSyncTime) {
        logger.info("2.1【开始】同步招标项目成交供应商");
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`bid_supplier_origin` \n" +
                "WHERE\n" +
                "\twin_bid_status = 1 \n" +
                "\tAND update_time >?";

        String querySql = "SELECT\n" +
                "\tcompany_id as companyId,\n" +
                "\tsub_project_id as projectId,\n" +
                "\tsupplier_id as supplierId,\n" +
                "\tsupplier_name as supplierName,\n" +
                "\tlink_man as linkManNotAnalyzed,\n" +
                "\tlink_phone as linkPhoneNotAnalyzed,\n" +
                "\tcreate_time as createTime,\n" +
                "\tsign_over as signOver\n" +
                "FROM\n" +
                "\t`bid_supplier_origin` \n" +
                "WHERE\n" +
                "\twin_bid_status = 1 \n" +
                "\tAND update_time >? \n" +
                "\tLIMIT ?,?";

        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealSupplierData(tenderDataSource, countSql, querySql, params, BIDDING_PROJECT_TYPE);
        logger.info("2.2【结束】同步招标项目成交供应商");
    }

    private void syncDealPurchaseProjectService(Timestamp lastSyncTime) {
        logger.info("1.1【开始】同步采购项目成交供应商");
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\t`purchase_supplier_project_origin` \n" +
                "WHERE\n" +
                "\tdeal_status = 2 \n" +
                "\tAND update_time > ?";

        String querySql = "SELECT\n" +
                "\tcompany_id as companyId,\n" +
                "\tproject_id as projectId,\n" +
                "\tsupplier_id as supplierId,\n" +
                "\tsupplier_name as supplierName,\n" +
                "\tlink_man as linkManNotAnalyzed,\n" +
                "\tlink_phone as linkPhoneNotAnalyzed,\n" +
                "\tcreate_time as createTime,\n" +
                "\tsign_over as signOver\n" +
                "FROM\n" +
                "\t`purchase_supplier_project_origin` \n" +
                "WHERE\n" +
                "\tdeal_status = 2 " +
                "AND update_time > ?" +
                "limit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealSupplierData(purchaseDataSource, countSql, querySql, params, PURCHASE_PROJECT_TYPE);
        logger.info("1.2【结束】同步采购项目成交供应商");
    }

    private void syncDealAuctionProjectService(Timestamp lastSyncTime) {
        logger.info("1.1【开始】同步采购项目成交供应商");
        String countSql = "SELECT\n" +
                "\tcount(1) \n" +
                "FROM\n" +
                "\t`auction_supplier_project_origin` \n" +
                "WHERE\n" +
                "\tdeal_status = 3 \n" +
                "\tAND update_time > ?";

        String querySql = "SELECT\n" +
                "\tcompany_id as companyId,\n" +
                "\tproject_id as projectId,\n" +
                "\tsupplier_id as supplierId,\n" +
                "\tsupplier_name as supplierName,\n" +
                "\tlink_man as linkManNotAnalyzed,\n" +
                "\tlink_phone as linkPhoneNotAnalyzed,\n" +
                "\tcreate_time as createTime,\n" +
                "\tsign_over as signOver\n" +
                "FROM\n" +
                "\t`auction_supplier_project_origin` \n" +
                "WHERE\n" +
                "\tdeal_status = 3 " +
                "AND update_time > ?" +
                "limit ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncDealSupplierData(auctionDataSource, countSql, querySql, params, AUCTION_PROJECT_TYPE);
        logger.info("1.2【结束】同步采购项目成交供应商");
    }

    private void doSyncDealSupplierData(DataSource dataSource, String countSql, String querySql, List<Object> params, int projectType) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.info("执行countSql:{}, 参数params:{}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.info("执行querySql:{}, 参数 paramsToUse:{}, 总条数:{}", querySql, paramsToUse, mapList.size());
                refresh(mapList, projectType);
                batchInsert(mapList);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder requestBuilder = elasticClient.getTransportClient().prepareBulk();
            mapList.forEach(map -> requestBuilder.add(elasticClient.getTransportClient().prepareIndex(
                    elasticClient.getProperties().getProperty("cluster.deal_supplier_index"),
                    elasticClient.getProperties().getProperty("cluster.type.deal_supplier")
                    , String.valueOf(map.get(ID))
            ).setSource(SyncTimeUtil.handlerDate(map))));

            BulkResponse bulkResponse = requestBuilder.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                logger.error(bulkResponse.buildFailureMessage());
            }
        }
    }

    private void refresh(List<Map<String, Object>> mapList, int projectType) {
        mapList.forEach(map -> {
            map.put(ID, generateDealSupplierId(map));
            map.put(COMPANY_ID, String.valueOf(map.get(COMPANY_ID)));
            map.put(SUPPLIER_ID, String.valueOf(map.get(SUPPLIER_ID)));
            map.put(PROJECT_TYPE, projectType);
            map.put(SUPPLIER_NAME_NOT_ANALYZED, map.get(SUPPLIER_NAME));
            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
            map.put(PROJECT_ID, String.valueOf(map.get(PROJECT_ID)));
            //添加平台来源
            map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.IXIETONG_SOURCE);
        });
    }

    private String generateDealSupplierId(Map<String, Object> result) {
        String companyId = String.valueOf(result.get(COMPANY_ID));
        Long supplierId = (Long) result.get(SUPPLIER_ID);
        if (supplierId == null) {
            throw new RuntimeException("采购商成交供应商ID生成失败，原因：供应商ID为空!");
        }
        if (companyId == null) {
            throw new RuntimeException("采购商成交供应商ID生成失败，原因：采购商ID为空!");
        }
        return DigestUtils.md5DigestAsHex((companyId + "_" + supplierId).getBytes());
    }
//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
