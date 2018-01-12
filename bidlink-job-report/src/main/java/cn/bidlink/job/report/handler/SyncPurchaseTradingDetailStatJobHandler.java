package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购成交明细统计
 * @Date 2017/12/26
 */
@Service
@JobHander("syncPurchaseTradingDetailJobHandler")
public class SyncPurchaseTradingDetailStatJobHandler extends SyncJobHandler implements InitializingBean {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseTradingDetailStatJobHandler.class);

    private String PROJECT_ID   = "project_id";
    private String ITEM_ID      = "item_id";
    private String USER_ID      = "user_id";
    private String DEPT         = "dept";
    private String COMPANY_ID   = "company_id";
    private String DIRECTORY_ID = "directory_id";
    private String SYNC_TIME    = "sync_time";

    private Map<String, Object> emptyDirectoryItem = new LinkedHashMap<>();

    /**
     * 表名
     *
     * @return
     */
    @Override
    protected String getTableName() {
        return "purchase_trading_detail_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购成交明细统计开始");
        syncPurchaseTradingDetail();
        // 记录同步时间
//        updateSyncLastTime();
        logger.info("同步采购成交明细统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseTradingDetail() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步采购流程项目概况列表统计lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        // 同步插入数据
        doSyncPurchaseTradingDetail(lastSyncTime);
    }

    /**
     * 同步插入数据
     *
     * @param lastSyncTime
     */
    private void doSyncPurchaseTradingDetail(Date lastSyncTime) {
        String countSql = "      SELECT\n"
                          + "         count(1)\n"
                          + "      FROM\n"
                          + "         bmpfjz_project bp\n"
                          + "      JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                          + "      AND bp.comp_id = bpe.comp_id\n"
                          + "      WHERE\n"
                          + "      bp.project_status = 9\n"
//                          + "      AND bp.comp_id = 1113172701\n"
                          + "      AND bp.create_time > ?\n"
                          + "      AND bpe.archive_time IS NOT NULL";

        String querySql = "SELECT\n"
                          + "         bp.id AS project_id,\n"
                          + "         bp.`name` AS project_name,\n"
                          + "         bp.comp_id AS company_id,\n"
                          + "         bp.user_id,\n"
                          + "         bp.user_name,\n"
                          + "         bp.department_code,\n"
                          + "         bp.user_name,\n"
                          + "         bp.create_time ,\n"
                          + "         bpe.archive_time\n"
                          + "      FROM\n"
                          + "         bmpfjz_project bp\n"
                          + "      JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id\n"
                          + "      AND bp.comp_id = bpe.comp_id\n"
                          + "      WHERE\n"
                          + "      bp.project_status = 9\n"
//                          + "      AND bp.comp_id = 1113172701\n"
                          + "      AND bp.create_time > ?\n"
                          + "      AND bpe.archive_time IS NOT NULL\t"
                          + "      ORDER BY\n"
                          + "         bpe.archive_time DESC\t"
                          + "   LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncCreate(ycDataSource, countSql, querySql, params);
    }

    /**
     * 同步插入数据
     *
     * @param dataSource
     * @param countSql
     * @param querySql
     * @param params
     */
    protected void syncCreate(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        Map<String, Map<String, Object>> projectList = null;
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                projectList = DBUtil.query(dataSource, querySql, paramsToUse, new DBUtil.ResultSetCallback<Map<String, Map<String, Object>>>() {
                    @Override
                    public Map<String, Map<String, Object>> execute(ResultSet resultSet) throws SQLException {
                        Map<String, Map<String, Object>> purchaseDetailMap = new HashMap<>();
                        while (resultSet.next()) {
                            Map<String, Object> map = new LinkedHashMap<>();
                            ResultSetMetaData metaData = resultSet.getMetaData();
                            int columnCount = metaData.getColumnCount();
                            for (int i = 0; i < columnCount; i++) {
                                String columnLabel = metaData.getColumnLabel(i + 1);
                                map.put(columnLabel, resultSet.getObject(columnLabel));
                            }
                            purchaseDetailMap.put(map.get(COMPANY_ID) + "_" + map.get(PROJECT_ID), map);
                        }
                        return purchaseDetailMap;
                    }
                });
                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, projectList.size());
                // 添加部门
                appendUserDept(projectList);
                // 添加采购品信息
                appendSupplierProjectItemBid(projectList);
            }
        }
    }

    private void appendUserDept(Map<String, Map<String, Object>> projectList) {
        Set<Pair> pairs = new HashSet<>();
        for (Map<String, Object> map : projectList.values()) {
            pairs.add(new Pair(((long) map.get(COMPANY_ID)), ((long) map.get(USER_ID))));
        }

        StringBuffer sb = new StringBuffer("SELECT id, company_id, dept FROM reg_user WHERE ");
        int count = 0;
        for (Pair pair : pairs) {
            if (count > 0) {
                sb.append(" OR ");
            }
            sb.append("(id=")
                    .append(pair.supplierId)
                    .append(" AND company_id=")
                    .append(pair.companyId)
                    .append(")");
            count++;
        }

        Map<String, String> userMap = DBUtil.query(ycDataSource, sb.toString(), null, new DBUtil.ResultSetCallback<Map<String, String>>() {
            @Override
            public Map<String, String> execute(ResultSet resultSet) throws SQLException {
                Map<String, String> map = new HashMap<String, String>();
                while (resultSet.next()) {
                    long userId = resultSet.getLong("id");
                    long companyId = resultSet.getLong("company_id");
                    String dept = resultSet.getString("dept");
                    String key = companyId + "_" + userId;
                    map.put(key, dept);
                }
                return map;
            }
        });

        for (Map<String, Object> map : projectList.values()) {
            String dept = map.get(COMPANY_ID) + "_" + map.get(USER_ID);
            map.put(DEPT, userMap.get(dept));
        }
    }

    private void appendSupplierProjectItemBid(Map<String, Map<String, Object>> projectList) {
        Set<Pair> pairs = new HashSet<>();
        for (Map<String, Object> map : projectList.values()) {
            pairs.add(new Pair(((long) map.get(COMPANY_ID)), ((long) map.get(PROJECT_ID))));
        }

        String countSql = "SELECT count(1) FROM bmpfjz_supplier_project_item_bid WHERE bid_status = 3 AND (";
        String querySql = "SELECT comp_id AS company_id,"
                          + "project_id, project_item_id, "
                          + "supplier_id, supplier_user_name AS supplier_name, "
                          + "bid_price, deal_price,"
                          + "deal_amount,"
                          + "deal_total_price "
                          + "FROM bmpfjz_supplier_project_item_bid WHERE bid_status = 3 AND (";
        final StringBuffer where = new StringBuffer();
        int index = 0;
        for (Pair pair : pairs) {
            if (index > 0) {
                where.append(" OR ");
            }
            where.append("(project_id=")
                    .append(pair.supplierId)
                    .append(" AND comp_id=")
                    .append(pair.companyId)
                    .append(")");
            index++;
        }
        where.append(")");

        countSql += where.toString();
        querySql += where.toString() + " LIMIT ?,?";

        long count = DBUtil.count(ycDataSource, countSql, null);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, null, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> params = appendToParams(new ArrayList<>(), i);
                final List<Pair> projectItemPair = new ArrayList<Pair>();
                final AtomicInteger atomicInteger = new AtomicInteger(0);
                Map<String, List<Map<String, Object>>> projectItemBidMap = DBUtil.query(ycDataSource, querySql, params, new DBUtil.ResultSetCallback<Map<String, List<Map<String, Object>>>>() {
                    @Override
                    public Map<String, List<Map<String, Object>>> execute(ResultSet resultSet) throws SQLException {
                        Map<String, List<Map<String, Object>>> map = new HashMap<>();
                        while (resultSet.next()) {
                            atomicInteger.incrementAndGet();
                            long companyId = resultSet.getLong("company_id");
                            long projectId = resultSet.getLong("project_id");
                            long projectItemId = resultSet.getLong("project_item_id");
                            long supplierId = resultSet.getLong("supplier_id");
                            String supplierName = resultSet.getString("supplier_name");
                            BigDecimal bidPrice = resultSet.getBigDecimal("bid_price");
                            BigDecimal dealPrice = resultSet.getBigDecimal("deal_price");
                            BigDecimal dealAmount = resultSet.getBigDecimal("deal_amount");
                            BigDecimal dealTotalPrice = resultSet.getBigDecimal("deal_total_price");
                            Map<String, Object> item = new LinkedHashMap<String, Object>();
                            item.put("company_id", companyId);
                            item.put("project_id", projectId);
                            item.put("item_id", projectItemId);
                            item.put("supplier_id", supplierId);
                            item.put("supplier_name", supplierName);
                            item.put("bid_price", bidPrice);
                            item.put("deal_price", dealPrice);
                            item.put("deal_amount", dealAmount);
                            item.put("deal_total_price", dealTotalPrice);
                            // companyId和projectId组合为key,对应多个item
                            String key = companyId + "_" + projectId;
                            List<Map<String, Object>> itemList = map.get(key);
                            if (itemList == null) {
                                itemList = new ArrayList<>();
                                map.put(key, itemList);
                            }
                            itemList.add(item);
                            // 添加供应商报价
                            projectItemPair.add(new Pair(companyId, projectItemId));
                        }
                        return map;
                    }
                });
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, params, atomicInteger.get());
                // 添加供应商报价信息
                appendProjectItem(projectItemBidMap, projectItemPair);

                // 合并项目与采购品
                List<Map<String, Object>> values = new ArrayList<>();
                for (Map.Entry<String, List<Map<String, Object>>> entry : projectItemBidMap.entrySet()) {
                    String key = entry.getKey();
                    Map<String, Object> project = projectList.get(key);
                    for (Map<String, Object> map : entry.getValue()) {
                        map.putAll(project);
                        values.add(map);
                    }
                }

                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(values)) {
                    StringBuilder sqlBuilder = sqlBuilder(values);
                    List<Object> insertParams = buildParams(values, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }

            }
        }
    }

    private void appendProjectItem(Map<String, List<Map<String, Object>>> projectItemBidMap, List<Pair> projectItemPair) {
        StringBuffer querySupplierBidSql = new StringBuffer("SELECT id AS item_id, `code` AS item_code,\n"
                                                            + " `name` AS item_name,\n"
                                                            + " spec,\n"
                                                            + " comp_id AS company_id,\n"
                                                            + " directory_id,\n"
                                                            + " project_id,\n"
                                                            + " unit_name,\n"
                                                            + " usedepart  FROM bmpfjz_project_item WHERE ");
        int index = 0;
        for (Pair pair : projectItemPair) {
            if (index > 0) {
                querySupplierBidSql.append(" OR ");
            }
            querySupplierBidSql.append("(id=")
                    .append(pair.supplierId)
                    .append(" AND comp_id=")
                    .append(pair.companyId)
                    .append(")");
            index++;
        }

        final List<Pair> directoryPair = new ArrayList<>();
        Map<String, Map<String, Object>> projectItemMap = DBUtil.query(ycDataSource, querySupplierBidSql.toString(), null, new DBUtil.ResultSetCallback<Map<String, Map<String, Object>>>() {
            @Override
            public Map<String, Map<String, Object>> execute(ResultSet resultSet) throws SQLException {
                Map<String, Map<String, Object>> map = new HashMap<>();
                while (resultSet.next()) {
                    long itemId = resultSet.getLong("item_id");
                    String itemCode = resultSet.getString("item_code");
                    String itemName = resultSet.getString("item_name");
                    String unitName = resultSet.getString("unit_name");
                    String usedepart = resultSet.getString("usedepart");
                    String spec = resultSet.getString("spec");
                    long companyId = resultSet.getLong("company_id");
                    long directoryId = resultSet.getLong("directory_id");
                    long projectId = resultSet.getLong("project_id");
                    // 组装每一行数据
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("item_id", itemId);
                    item.put("item_code", itemCode);
                    item.put("item_name", itemName);
                    item.put("unit_name", unitName);
                    item.put("usedepart", usedepart);
                    item.put("spec", spec);
                    item.put("company_id", companyId);
                    item.put("directory_id", directoryId);
                    item.put("project_id", projectId);
                    // companyId和projectId组合为key,对应多个item
                    String key = companyId + "_" + projectId + "_" + itemId;
                    map.put(key, item);
                    // 采购品目录
                    directoryPair.add(new Pair(companyId, directoryId));
                }
                return map;
            }
        });

        // 添加采购品目录
        appendDirectory(projectItemMap, directoryPair);

        for (List<Map<String, Object>> projectItemBid : projectItemBidMap.values()) {
            for (Map<String, Object> map : projectItemBid) {
                String key = map.get(COMPANY_ID) + "_" + map.get(PROJECT_ID) + "_" + map.get(ITEM_ID);
                Map<String, Object> projectItem = projectItemMap.get(key);
                if (projectItem != null) {
                    map.putAll(projectItem);
                } else {
                    logger.info("没找到对应的projectItem数据，company_id, project_id, item_id : {}", key);
                }
            }
        }
    }


    private void appendDirectory(Map<String, Map<String, Object>> supplierBidMap, List<Pair> directoryPair) {
        StringBuffer queryDirectorySql = new StringBuffer("SELECT id AS directory_id, company_id, catalog_id, catalog_name FROM corp_directorys WHERE ");
        int index = 0;
        for (Pair pair : directoryPair) {
            if (index > 0) {
                queryDirectorySql.append(" OR ");
            }
            queryDirectorySql.append("(id=")
                    .append(pair.supplierId)
                    .append(" AND company_id=")
                    .append(pair.companyId)
                    .append(")");
            index++;
        }

        Map<String, Map<String, Object>> directoryMap = DBUtil.query(ycDataSource, queryDirectorySql.toString(), null, new DBUtil.ResultSetCallback<Map<String, Map<String, Object>>>() {
            @Override
            public Map<String, Map<String, Object>> execute(ResultSet resultSet) throws SQLException {
                Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();
                while (resultSet.next()) {
                    long directoryId = resultSet.getLong("directory_id");
                    long companyId = resultSet.getLong("company_id");
                    long catalogId = resultSet.getLong("catalog_id");
                    String catalogName = resultSet.getString("catalog_name");
                    Map<String, Object> item = new LinkedHashMap<String, Object>();
                    item.put("directory_id", directoryId);
                    item.put("company_id", companyId);
                    item.put("catalog_id", catalogId);
                    item.put("catalog_name", catalogName);
                    String key = companyId + "_" + directoryId;
                    map.put(key, item);
                }
                return map;
            }
        });

        int emptyDirectoryCount = 0;
        for (Map<String, Object> map : supplierBidMap.values()) {
            String key = map.get(COMPANY_ID) + "_" + map.get(DIRECTORY_ID);
            Map<String, Object> directoryItem = directoryMap.get(key);
            if (directoryItem != null) {
                map.putAll(directoryItem);
            } else {
                emptyDirectoryCount++;
                map.putAll(emptyDirectoryItem);
            }
        }
        logger.info("空采购品目录：" + emptyDirectoryCount);
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        emptyDirectoryItem.put("directory_id", null);
        emptyDirectoryItem.put("company_id", null);
        emptyDirectoryItem.put("catalog_id", null);
        emptyDirectoryItem.put("catalog_name", null);

        execute();
    }
}
