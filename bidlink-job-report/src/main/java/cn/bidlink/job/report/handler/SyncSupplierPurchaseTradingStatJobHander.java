package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:供应商采购成交统计
 * @Date 2018/1/9
 */
@Service
@JobHander("syncSupplierPurchaseTradingStatJobHander")
public class SyncSupplierPurchaseTradingStatJobHander extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncSupplierPurchaseTradingStatJobHander.class);

    @Override
    protected String getTableName() {
        return "supplier_purchase_trading_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步供应商采购成交统计开始");
        syncSupplierPurchaseDeal();
        updateSyncLastTime();
        logger.info("同步供应商采购成交统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncSupplierPurchaseDeal() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步供应商采购成交统计 lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        syncTraySupplierPurchaseDeal(lastSyncTime);
        syncOutSideSupplierPurchaseDeal(lastSyncTime);
    }

    /**
     * 同步盘外供应商
     *
     * @param lastSyncTime
     */
    private void syncOutSideSupplierPurchaseDeal(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbsm_company_supplier_apply bcs\n" +
                "LEFT JOIN bmpfjz_supplier_project_bid spb ON bcs.supplier_id = spb.supplier_id\n" +
                "AND bcs.company_id = spb.comp_id\n" +
                "JOIN bmpfjz_project p ON spb.project_id = p.id\n" +
                "AND spb.comp_id = p.comp_id\n" +
                "WHERE\n" +
                "\tspb.supplier_bid_status IN (2, 3, 6, 7)\n" +
                "AND bcs.supplier_status != 4\n" +
                "AND p.create_time > ?";
        String querySql = "SELECT\n" +
                "\tp.comp_id AS company_id,\n" +
                "\tp.id AS project_id,\n" +
                "\tspb.supplier_id,\n" +
                "\tspb.supplier_name,\n" +
                "\tdeal_total_price,\n" +
                "\tbcs.supplier_code,\n" +
                "\tbcs.supplier_industry,\n" +
                "\tbcs.supplier_region,\n" +
                "\tbcs.supplier_status,\n" +
                "\tspb.supplier_bid_status,\n" +
                "\tp.create_time\n" +
                "FROM\n" +
                "\tbsm_company_supplier_apply bcs\n" +
                "LEFT JOIN bmpfjz_supplier_project_bid spb ON bcs.supplier_id = spb.supplier_id\n" +
                "AND bcs.company_id = spb.comp_id\n" +
                "JOIN bmpfjz_project p ON spb.project_id = p.id\n" +
                "AND spb.comp_id = p.comp_id\n" +
                "WHERE\n" +
                "\tspb.supplier_bid_status IN (2, 3, 6, 7)\n" +
                "AND bcs.supplier_status != 4\n" +
                "AND p.create_time > ?\n" +
                "LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncOutside(ycDataSource, countSql, querySql, params);
    }

    /**
     * 同步盘内供应商
     *
     * @param lastSyncTime
     */
    private void syncTraySupplierPurchaseDeal(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbsm_company_supplier bcs\n" +
                "LEFT JOIN bmpfjz_supplier_project_bid spb ON bcs.supplier_id = spb.supplier_id\n" +
                "AND bcs.company_id = spb.comp_id\n" +
                "JOIN bmpfjz_project p ON spb.project_id = p.id\n" +
                "AND spb.comp_id = p.comp_id\n" +
                "WHERE\n" +
                "\tspb.supplier_bid_status IN (2, 3, 6, 7) AND p.create_time > ?";
        String querySql = "SELECT\n" +
                "\tspb.comp_id AS company_id,\n" +
                "\tspb.project_id,\n" +
                "\tspb.supplier_id,\n" +
                "\tspb.supplier_name,\n" +
                "\tdeal_total_price,\n" +
                "\tbcs.supplier_code,\n" +
                "\tbcs.supplier_industry,\n" +
                "\tbcs.supplier_region,\n" +
                "\tspb.supplier_bid_status,\n" +
                "\tp.create_time\n" +
                "FROM\n" +
                "\tbsm_company_supplier bcs\n" +
                "LEFT JOIN bmpfjz_supplier_project_bid spb ON bcs.supplier_id = spb.supplier_id\n" +
                "AND bcs.company_id = spb.comp_id\n" +
                "JOIN bmpfjz_project p ON spb.project_id = p.id\n" +
                "AND spb.comp_id = p.comp_id\n" +
                "WHERE\n" +
                "\tspb.supplier_bid_status IN (2, 3, 6, 7)" +
                "\tAND p.create_time > ?" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncTray(ycDataSource, countSql, querySql, params);
    }

    private void syncTray(DataSource dataSource, String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());

                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = sqlBuilder(mapList);
                    // supplierType 1:盘内  0:盘外
                    List<Object> insertParams = buildParams(mapList, sqlBuilder,1);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    private void syncOutside(DataSource dataSource, String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {} , params : {} , 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {} , params : {} , 共{}条", querySql, params, mapList.size());

                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = sqlBuilder(mapList);
                    // supplierType 1:盘内  0:盘外
                    List<Object> insertParams = buildParams(mapList, sqlBuilder, 0);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    @Override
    protected StringBuilder sqlBuilder(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(getTableName()).append(" (");
        int columnIndex = 0;
        for (String columnName : mapList.get(0).keySet()) {
            if (columnIndex > 0) {
                sqlBuilder.append(",").append(columnName);
            } else {
                sqlBuilder.append(columnName);
            }
            columnIndex++;
        }
        sqlBuilder.append(", sync_time , supplier_type ) VALUES ");
        return sqlBuilder;
    }

    private List<Object> buildParams(List<Map<String, Object>> mapList, StringBuilder sqlBuilder, Integer supplierType) {
        List<Object> insertParams = new ArrayList<>();
        int listIndex = 0;
        for (Map<String, Object> map : mapList) {
            if (listIndex > 0) {
                sqlBuilder.append(", (");
            } else {
                sqlBuilder.append(" (");
            }
            // 添加同步时间字段和供应商类型字段
            int size = map.values().size() + 2;
            for (int j = 0; j < size; j++) {
                if (j > 0) {
                    sqlBuilder.append(", ?");
                } else {
                    sqlBuilder.append("?");
                }
            }
            sqlBuilder.append(")");
            insertParams.addAll(map.values());
            insertParams.add(SyncTimeUtil.getCurrentDate());
            // supplierType 1:盘内  0:盘外
            insertParams.add(supplierType);
            listIndex++;
        }
        return insertParams;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
