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
 * @description:同步采购流程项目概况列表统计
 * @Date 2017/12/26
 */
@Service
@JobHander("syncPurchaseProcessStatJobHandler")
public class SyncPurchaseProcessStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {


    private Logger logger = LoggerFactory.getLogger(SyncPurchaseProcessStatJobHandler.class);

    private String PROJECT_ID = "project_id";
    private String COMPANY_ID = "company_id";
    private String DIRECTORY_ID = "directory_id";
    private String SYNC_TIME = "sync_time";

    /**
     * 表名
     *
     * @return
     */
    @Override
    protected String getTableName() {
        return "purchase_process_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购流程项目概况列表统计开始");
        syncPurchaseProcess();
        logger.info("同步采购流程项目概况列表统计结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购流程项目概况列表统计
     */
    private void syncPurchaseProcess() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步采购流程项目概况列表统计lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        // 同步插入数据
        syncCreatePurchaseProcess(lastSyncTime);

        // 同步更新数据
        syncUpdatePurchaseProcess(lastSyncTime);

        // 记录上次同步时间
        syncRecord();
    }



    /**
     * 同步更新数据
     *
     * @param lastSyncTime
     */
    private void syncUpdatePurchaseProcess(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tbp.id\n" +
                "\t\tFROM\n" +
                "\t\t\tbmpfjz_project bp,\n" +
                "\t\t\tBMPFJZ_PROJECT_EXT bpe\n" +
                "\t\tWHERE\n" +
                "\t\t\tbpe.COMP_ID = bp.COMP_ID\n" +
                "\t\tAND bpe.ID = bp.ID\n" +
                "AND bp.update_time > ?\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tbpe.COMP_ID,\n" +
                "\t\t\tbp.id\n" +
                "\t) AS s";

        String querySql = "SELECT\n" +
                "\tSUM(\n" +
                "\t\tIFNULL(bpe.DEAL_TOTAL_PRICE, 0)\n" +
                "\t) AS sum_total_price,\n" +
                "\tbp.stop_status,\n" +
                "\tbp.project_status,\n" +
                "\tbp.department_code,\n" +
                "\tbpe.publish_bid_result_time,\n" +
                "\tbp.create_time,\n" +
                "\tbpe.comp_id AS company_id,\n" +
                "\tbp.id AS project_id\n" +
                "FROM\n" +
                "\tbmpfjz_project bp,\n" +
                "\tBMPFJZ_PROJECT_EXT bpe\n" +
                "WHERE\n" +
                "\tbpe.COMP_ID = bp.COMP_ID\n" +
                "AND bpe.ID = bp.ID\n" +
                "AND bp.update_time > ?\n" +
                "GROUP BY\n" +
                "\tbpe.COMP_ID,\n" +
                "\tbp.id" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncUpdate(ycDataSource, countSql, querySql, params);

    }

    /**
     * 同步插入数据
     *
     * @param lastSyncTime
     */
    private void syncCreatePurchaseProcess(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tbp.id\n" +
                "\t\tFROM\n" +
                "\t\t\tbmpfjz_project bp,\n" +
                "\t\t\tBMPFJZ_PROJECT_EXT bpe\n" +
                "\t\tWHERE\n" +
                "\t\t\tbpe.COMP_ID = bp.COMP_ID\n" +
                "\t\tAND bpe.ID = bp.ID\n" +
                "AND bp.create_time > ?\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tbpe.COMP_ID,\n" +
                "\t\t\tbp.id\n" +
                "\t) AS s";

        String querySql = "SELECT\n" +
                "\tSUM(\n" +
                "\t\tIFNULL(bpe.DEAL_TOTAL_PRICE, 0)\n" +
                "\t) AS sum_total_price,\n" +
                "\tbp.stop_status,\n" +
                "\tbp.project_status,\n" +
                "\tbp.department_code,\n" +
                "\tbpe.publish_bid_result_time,\n" +
                "\tbp.create_time,\n" +
                "\tbpe.comp_id AS company_id,\n" +
                "\tbp.id AS project_id\n" +
                "FROM\n" +
                "\tbmpfjz_project bp,\n" +
                "\tBMPFJZ_PROJECT_EXT bpe\n" +
                "WHERE\n" +
                "\tbpe.COMP_ID = bp.COMP_ID\n" +
                "AND bpe.ID = bp.ID\n" +
                "AND bp.create_time > ?\n" +
                "GROUP BY\n" +
                "\tbpe.COMP_ID,\n" +
                "\tbp.id" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncCreate(ycDataSource, countSql, querySql, params);


    }

    /**
     *同步更新数据
     *
     * @param dataSource
     * @param countSql
     * @param querySql
     * @param params
     */
    protected void syncUpdate(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行更新countSql : {}, params : {}，共{}条", countSql, params, count);
        List<Map<String, Object>> mapList = null;
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                logger.debug("执行更新querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());

                // 添加采购品id
                appendDirectoryIds(mapList);
                // 生成update sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = updateSqlBuilder(mapList);
                    updateBuildParams(mapList);
                    DBUtil.batchExecute(reportDataSource, sqlBuilder.toString(), mapList);
                }
            }

        }
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
        logger.debug("执行插入countSql : {}, params : {}，共{}条", countSql, params, count);
        List<Map<String, Object>> mapList = null;
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                logger.debug("执行插入querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());

                // 添加采购品id
                appendDirectoryIds(mapList);
                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = sqlBuilder(mapList);
                    List<Object> insertParams = buildParams(mapList, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);

                }
            }

        }
    }


    /**
     * 添加采购品id
     *
     * @param mapList
     */
    private void appendDirectoryIds(List<Map<String, Object>> mapList) {
        Map<Object, Object> map = new HashMap<>();

        for (Map<String, Object> mapAttr : mapList) {
            map.put(mapAttr.get(PROJECT_ID), mapAttr.get(COMPANY_ID));
        }

        StringBuilder sumDirectorySqlBuilder = new StringBuilder();
        sumDirectorySqlBuilder.append("SELECT\n" +
                "\tbpi.directory_id ,bpi.project_id, bpi.comp_id AS company_id\n" +
                "FROM\n" +
                "\tbmpfjz_project_item bpi\n" +
                "WHERE (\n");

        int conditionIndex = 0;
        for (Map.Entry<Object, Object> attr : map.entrySet()) {
            if (conditionIndex > 0) {
                sumDirectorySqlBuilder.append(" or").append("(bpi.project_id =").append("" + attr.getKey())
                        .append(" AND bpi.comp_id = ").append(attr.getValue() + ")");
            } else {
                sumDirectorySqlBuilder.append("(bpi.project_id =").append("" + attr.getKey())
                        .append(" AND bpi.comp_id = ").append(attr.getValue() + ")");
            }
            conditionIndex++;
        }
        sumDirectorySqlBuilder.append(") AND (\n" +
                "\tbpi.WIN_BID_STATUS = 1\n" +
                "\tOR bpi.WIN_BID_STATUS = 2\n" +
                ")\n");

        final Map<String, StringBuilder> directorysMap = DBUtil.query(ycDataSource, sumDirectorySqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, StringBuilder>>() {
            @Override
            public Map<String, StringBuilder> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, StringBuilder> map = new HashMap<>();
                while (resultSet.next()) {
                    long projectId = resultSet.getLong(PROJECT_ID);
                    long companyId = resultSet.getLong(COMPANY_ID);
                    String key = projectId + "_" + companyId;
                    StringBuilder directorys = map.get(key);
                    if (directorys == null) {
                        directorys = new StringBuilder();
                        map.put(key, directorys);
                        directorys.append(resultSet.getLong(DIRECTORY_ID));
                    } else {
                        directorys.append(",").append(resultSet.getLong(DIRECTORY_ID));
                    }
                }
                return map;
            }
        });

        for (Map<String, Object> mapAttr : mapList) {
            Object projectId = mapAttr.get(PROJECT_ID);
            Object companyId = mapAttr.get(COMPANY_ID);
            String key = projectId + "_" + companyId;
            StringBuilder directorys = directorysMap.get(key);
            if (directorys != null) {
                mapAttr.put("directory_ids", directorys.toString());
            } else {
                mapAttr.put("directory_ids", null);
            }
        }
    }

    private StringBuilder updateSqlBuilder(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("UPDATE ").append(getTableName()).append(" SET ");
        int columnIndex = 0;
        for (String columnName : mapList.get(0).keySet()) {
            if (columnIndex == 0) {
                sqlBuilder.append(columnName).append("=?");
            } else if (columnIndex > 0 && !(COMPANY_ID.equals(columnName) || PROJECT_ID.equals(columnName))) {
                sqlBuilder.append(",").append(columnName).append("=?");
            }
            columnIndex++;
        }
        sqlBuilder.append(", sync_time=? where company_id = ? AND project_id = ? ");
        return sqlBuilder;
    }


    private void updateBuildParams(List<Map<String, Object>> mapList) {
        for (Map<String, Object> map : mapList) {
            Object companyId = map.remove(COMPANY_ID);
            Object projectId = map.remove(PROJECT_ID);
            map.put(SYNC_TIME, SyncTimeUtil.getCurrentDate());
            map.put(COMPANY_ID, companyId);
            map.put(PROJECT_ID, projectId);
        }
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//        String sql = "SELECT\n" +
//                "\tpps.directory_ids\n" +
//                "FROM\n" +
//                "\tpurchase_process_stat pps\n" +
//                "WHERE\n" +
//                "\tpps.company_id = 1113172702\n" +
//                "AND pps.project_status IN (8, 9)\n" +
//                "AND pps.publish_bid_result_time BETWEEN '2017-11-27 00:00:00'\n" +
//                "AND '2017-12-27 00:00:00'";
//        Integer sumDirectory = DBUtil.query(reportDataSource, sql, null, new DBUtil.ResultSetCallback<Integer>() {
//            @Override
//            public Integer execute(ResultSet resultSet) throws SQLException {
//                HashSet<String> sumDirectory = new HashSet<>();
//                while (resultSet.next()) {
//                    String directory_ids = resultSet.getString("directory_ids");
//                    String[] directoryIds = directory_ids.split(",");
//                    for (String directoryId : directoryIds) {
//                        sumDirectory.add(directoryId);
//                    }
//                }
//                return sumDirectory.size();
//            }
//        });
//        System.out.println("=============="+sumDirectory);
//    }
}
