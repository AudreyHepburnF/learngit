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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购进展情况报表
 * @Date 2018/1/3
 */
@Service
@JobHander("syncPurchaseProcessStatJobHandler")
public class SyncPurchaseProcessStatJobHandler extends SyncJobHandler/* implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseProcessStatJobHandler.class);
    private String PROJECT_ID = "project_id";

    @Override
    protected String getTableName() {
        return "purchase_process_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购进展情况报表统计开始");
        syncPurchaseStatusProcess();
        // 记录同步信息
        updateSyncLastTime();
        logger.info("同步采购进展情况报表统计结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购进展情况
     */
    private void syncPurchaseStatusProcess() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步采购进展情况报表lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        syncCreatePurchaseProcess(lastSyncTime);
        syncUpdatePurchaseProcess(lastSyncTime);
    }

    private void syncCreatePurchaseProcess(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project\n" +
                "WHERE\n" +
                "\t(\n" +
                "\t\tproject_status != 10\n" +
                "\t\tOR project_status IS NOT NULL\n" +
                "\t)\n" +
                "AND project_status != 10\n" +
                "AND create_time > ?\n";
        String querySql = "SELECT\n" +
                "\tproject_status,\n" +
                "\tcomp_id AS company_id,\n" +
                "\tid AS project_id,\n" +
                "\tcreate_time\n" +
                "FROM\n" +
                "\tbmpfjz_project\n" +
                "WHERE\n" +
                "\t(\n" +
                "\t\tproject_status != 10\n" +
                "\t\tOR project_status IS NOT NULL\n" +
                "\t)\n" +
                "AND project_status != 10\n" +
                "AND create_time > ?\n" +
                "LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }

    private void syncUpdatePurchaseProcess(Date lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project\n" +
                "WHERE\n" +
                "\t(\n" +
                "\t\tproject_status != 10\n" +
                "\t\tOR project_status IS NOT NULL\n" +
                "\t)\n" +
                "AND project_status != 10\n" +
                "AND update_time > ?";
        String querySql = "SELECT\n" +
                "\tproject_status,\n" +
                "\tcomp_id AS company_id,\n" +
                "\tid AS project_id,\n" +
                "\tcreate_time\n" +
                "FROM\n" +
                "\tbmpfjz_project\n" +
                "WHERE\n" +
                "\t(\n" +
                "\t\tproject_status != 10\n" +
                "\t\tOR project_status IS NOT NULL\n" +
                "\t)\n" +
                "AND project_status != 10\n" +
                "AND update_time >?" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        syncUpdate(ycDataSource, countSql, querySql, params);
    }

    private void syncUpdate(DataSource ycDataSource, String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(ycDataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(ycDataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, paramsToUse : {} ,共{}条", querySql, paramsToUse, mapList.size());

                if (!CollectionUtils.isEmpty(mapList)) {
                    // 生成 update sql,参数
                    StringBuilder updateSql = updateSqlBuilder(mapList);
                    updateParamBuilder(mapList);
                    DBUtil.batchExecute(reportDataSource, updateSql.toString(), mapList);
                }
            }
        }
    }

    private void updateParamBuilder(List<Map<String, Object>> mapList) {
        for (Map<String, Object> map : mapList) {
            Object projectId = map.remove(PROJECT_ID);
            Timestamp currentDate = SyncTimeUtil.getCurrentDate();
            map.put("sync_time", currentDate);
            map.put(PROJECT_ID, projectId);
        }
    }

    private StringBuilder updateSqlBuilder(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(" UPDATE ").append(getTableName()).append(" SET ");
        int columnIndex = 0;
        for (String columnName : mapList.get(0).keySet()) {
            if (columnIndex == 0) {
                sqlBuilder.append(columnName).append(" = ?");
            } else if (columnIndex > 0 && !columnName.equals(PROJECT_ID)) {
                sqlBuilder.append(" , ").append(columnName).append(" = ?");
            }
            columnIndex++;
        }
        //添加同步时间字段
        sqlBuilder.append(",sync_time=? WHERE " + PROJECT_ID + " = ?");
        return sqlBuilder;
    }


//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
