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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购成交采购品报表统计
 * @Date 2018/1/9
 */
@Service
@JobHander("syncPurchaseTradingProductStatJobHandler")
public class SyncPurchaseTradingProductStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseTradingProductStatJobHandler.class);

    private String COMPANY_ID = "company_id";
    private String PROJECT_ID = "project_id";
    private String DIRECTORY_ID = "directory_id";
    private String ITEM_NAME = "item_name";
    private String ITEM_CODE = "item_code";
    private String ITEM_UNIT = "item_unit";
    private String ITEM_PARAMS = "item_params";
    private String ITEM_SPEC = "item_spec";
    private String CATALOG_ID = "catalog_id";

    @Override
    protected String getTableName() {
        return "purchase_trading_product_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("采购成交采购品报表统计开始");
        syncPurchaseTradingProduct();
        updateSyncLastTime();
        logger.info("采购成交采购品报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseTradingProduct() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("采购成交采购品报表统计 lastSyncTime : " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project bp\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9)\n" +
                "AND bp.create_time > ?\n";
        String querySql = "SELECT\n" +
                "\tbp.id AS project_id,\n" +
                "\tbp.comp_id AS company_id,\n" +
                "\tbp.create_time\n" +
                "FROM\n" +
                "\tbmpfjz_project bp\n" +
                "WHERE\n" +
                "\tbp.project_status IN (8, 9)\n" +
                "AND bp.create_time > ?\n" +
                "LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }

    @Override
    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                // 1.添加供应商报表信息 和 采购目录信息  批量插入数据
                appendSupplierOfferInfo(mapList);
            }
        }
    }

    private void appendSupplierOfferInfo(List<Map<String, Object>> mapList) {
        // 封装项目ID和公司ID
        HashMap<String, Object> projectMap = new HashMap<>();
        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            projectMap.put(key, map);
        }

        // 拼接供应商报价信息总条数sql
        StringBuilder countSqlBuilder = new StringBuilder();
        countSqlBuilder.append("SELECT COUNT(1) FROM bmpfjz_supplier_project_item_bid");
        countSqlBuilder.append(supplierConditionSqlBuilder(projectMap));

        // 拼接供应商报价信息分页查询结果集sql
        StringBuilder querySqlBuilder = new StringBuilder();
        querySqlBuilder.append("SELECT\tdeal_price,deal_total_price,directory_id,win_bid_time,project_id,comp_id AS company_id\tFROM\tbmpfjz_supplier_project_item_bid");
        querySqlBuilder.append(supplierConditionSqlBuilder(projectMap));
        querySqlBuilder.append("LIMIT ?,?");

        // 查询供应商报价信息总条数和结果集
        long count = DBUtil.count(ycDataSource, countSqlBuilder.toString(), null);
        logger.info("查询供应商报价信息共{}条", count);
        if (count > 0) {
            for (long i = 0; i <= count; i += pageSize) {
                ArrayList<Object> params = new ArrayList<>();
                params.add(i);
                params.add(pageSize);
                List<Map<String, Object>> supplierInfoListMap = DBUtil.query(ycDataSource, querySqlBuilder.toString(), params);
                logger.info("执行供应商报价信息querySql, params :{} ,共{}条",params,supplierInfoListMap.size());

                // 封装采购品目录ID和公司ID
                HashMap<String, Object> directoryIdAndCompIdMap = new HashMap<>();
                for (Map<String, Object> map : supplierInfoListMap) {
                    Object companyId = map.get(COMPANY_ID);
                    Object directoryId = map.get(DIRECTORY_ID);
                    String key = directoryId + "_" + companyId;
                    directoryIdAndCompIdMap.put(key, 1);
                }

                // 查询采购品信息
                final HashMap<String, Map<String, Object>> directoryMap = new HashMap<>();
                String queryDirectorySql = directoryConditionSqlBuilder(directoryIdAndCompIdMap).toString();
                DBUtil.query(ycDataSource, queryDirectorySql, null, new DBUtil.ResultSetCallback<Void>() {
                    @Override
                    public Void execute(ResultSet resultSet) throws SQLException {
                        while (resultSet.next()) {
                            long directoryId = resultSet.getLong(DIRECTORY_ID);
                            long companyId = resultSet.getLong(COMPANY_ID);
                            String itemName = resultSet.getString(ITEM_NAME);
                            String itemCode = resultSet.getString(ITEM_CODE);
                            String itemParams = resultSet.getString(ITEM_PARAMS);
                            String itemSpec = resultSet.getString(ITEM_SPEC);
                            String itemUnit = resultSet.getString(ITEM_UNIT);
                            long catalogId = resultSet.getLong(CATALOG_ID);

                            HashMap<String, Object> map = new HashMap<>();
                            map.put(ITEM_NAME, itemName);
                            map.put(ITEM_CODE, itemCode);
                            map.put(ITEM_PARAMS, itemParams);
                            map.put(ITEM_SPEC, itemSpec);
                            map.put(ITEM_UNIT, itemUnit);
                            map.put(CATALOG_ID, catalogId);
                            String key = directoryId + "_" + companyId;
                            directoryMap.put(key, map);
                        }
                        return null;
                    }
                });

                // 添加采购项目和采购品信息
                for (Map<String, Object> map : supplierInfoListMap) {
                    Object companyId = map.get(COMPANY_ID);
                    Object directoryId = map.get(DIRECTORY_ID);
                    Object projectId = map.get(PROJECT_ID);

                    // 添加采购项目信息
                    String projectInfoKey = projectId + "_" + companyId;
                    Map<String, Object> projectInfoMap = ((Map<String, Object>) projectMap.get(projectInfoKey));
                    Object createTime = projectInfoMap.get("create_time");
                    map.put("create_time", createTime);

                    // 添加采购品信息
                    String key = directoryId + "_" + companyId;
                    Map<String, Object> directoryInfoMap = directoryMap.get(key);
                    if (!CollectionUtils.isEmpty(directoryInfoMap)) {
                        Object itemName = directoryInfoMap.get(ITEM_NAME);
                        Object itemCode = directoryInfoMap.get(ITEM_CODE);
                        Object itemParams = directoryInfoMap.get(ITEM_PARAMS);
                        Object itemSpec = directoryInfoMap.get(ITEM_SPEC);
                        Object itemUnit = directoryInfoMap.get(ITEM_UNIT);
                        Object catalogId = directoryInfoMap.get(CATALOG_ID);
                        map.put(ITEM_NAME, itemName);
                        map.put(ITEM_CODE, itemCode);
                        map.put(ITEM_PARAMS, itemParams);
                        map.put(ITEM_SPEC, itemSpec);
                        map.put(ITEM_UNIT, itemUnit);
                        map.put(CATALOG_ID, catalogId);
                    } else {
                        map.put(ITEM_NAME, null);
                        map.put(ITEM_CODE, null);
                        map.put(ITEM_PARAMS, null);
                        map.put(ITEM_SPEC, null);
                        map.put(ITEM_UNIT, null);
                        map.put(CATALOG_ID, null);
                    }
                }

                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(supplierInfoListMap)) {
                    StringBuilder sqlBuilder = sqlBuilder(supplierInfoListMap);
                    List<Object> insertParams = buildParams(supplierInfoListMap, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    /**
     * 拼接供应商信息查询条件
     *
     * @param projectIdAndCompIdMap
     * @return
     */
    private StringBuilder supplierConditionSqlBuilder(HashMap<String, Object> projectIdAndCompIdMap) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" WHERE (");
        int columnIndex = 0;
        for (String key : projectIdAndCompIdMap.keySet()) {
            String projectId = key.split("_")[0];
            String companyId = key.split("_")[1];
            if (columnIndex > 0) {
                sqlBuilder.append(" OR ( ").append(" comp_id = ").append(companyId)
                        .append(" AND ").append(" project_id = ").append(projectId).append(")");
            } else {
                sqlBuilder.append(" ( ").append(" comp_id = ").append(companyId)
                        .append(" AND ").append(" project_id = ").append(projectId).append(")");

            }
            columnIndex++;
        }
        sqlBuilder.append(")");
        sqlBuilder.append("AND bid_status = 3 ");
        return sqlBuilder;
    }


    /**
     * 拼接采购品信息查询条件
     *
     * @param directoryIdAndCompIdMap
     * @return
     */
    private StringBuilder directoryConditionSqlBuilder(HashMap<String, Object> directoryIdAndCompIdMap) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT cd.`NAME` AS item_name,cd.`CODE` AS item_code,cd.UNITNAME AS item_unit,cd.tech_parameters AS item_params,cd.SPEC AS item_spec,cd.catalog_id,cd.company_id,cd.id AS directory_id FROM corp_directorys cd");
        sqlBuilder.append(" WHERE (");
        int columnIndex = 0;
        for (String key : directoryIdAndCompIdMap.keySet()) {
            String directoryId = key.split("_")[0];
            String companyId = key.split("_")[1];
            if (columnIndex > 0) {
                sqlBuilder.append(" OR ( ").append("company_id = ").append(companyId)
                        .append(" AND ").append(" ID = ").append(directoryId).append(")");
            } else {
                sqlBuilder.append(" ( ").append(" company_id = ").append(companyId)
                        .append(" AND ").append(" ID = ").append(directoryId).append(")");

            }
            columnIndex++;
        }
        sqlBuilder.append(")");
        sqlBuilder.append(" AND cd.ABANDON =1");
        return sqlBuilder;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
