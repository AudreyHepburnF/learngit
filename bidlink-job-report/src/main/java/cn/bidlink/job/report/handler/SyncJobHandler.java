package cn.bidlink.job.report.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.handler.IJobHandler;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
public abstract class SyncJobHandler extends IJobHandler {
    private Logger logger = LoggerFactory.getLogger(SyncJobHandler.class);

    @Autowired
    @Qualifier("ycDataSource")
    protected DataSource ycDataSource;

    @Autowired
    @Qualifier("reportDataSource")
    protected DataSource reportDataSource;

    @Value("${pageSize:200}")
    protected int pageSize;

    /**
     * 获取表名
     *
     * @return
     */
    protected abstract String getTableName();

    /**
     * 获取零点时间
     *
     * @return
     */
    protected Date getZeroTime() {
        DateTime dateTime = new DateTime();
        DateTime zeroTime = dateTime.minusHours(dateTime.getHourOfDay())
                .minusMinutes(dateTime.getMinuteOfHour())
                .minusSeconds(dateTime.getSecondOfMinute())
                .minusMillis(dateTime.getMillisOfSecond());
        return zeroTime.toDate();
    }

    /**
     * 清空表数据
     */
    protected void clearTableData() {
        String clearSql = "DELETE FROM %s";
        DBUtil.execute(reportDataSource, String.format(clearSql, getTableName()), null);
    }

    protected Date getLastSyncTime() {
        String querySql = "SELECT sync_time FROM sync_record WHERE table_name = ?";
        List<Object> params = new ArrayList<>();
        params.add(getTableName());
        List<Map<String, Object>> mapList = DBUtil.query(reportDataSource, querySql, params);
        if (CollectionUtils.isEmpty(mapList)) {
            return SyncTimeUtil.GMT_TIME;
        } else {
            return (Date) mapList.get(0).get("sync_time");
        }
    }


    protected void updateSyncLastTime() {
        String sql = "";
        ArrayList<Object> params = new ArrayList<>();
        if (getLastSyncTime() == SyncTimeUtil.GMT_TIME) {
            sql = "insert into sync_record(sync_time,table_name) value(?,?)";
        } else {
            sql = "update sync_record set sync_time = ? where table_name = ?";
        }
        params.add(SyncTimeUtil.getCurrentDate());
        params.add(getTableName());
        DBUtil.execute(reportDataSource, sql, params);
    }

    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
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
                    List<Object> insertParams = buildParams(mapList, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    /**
     * 添加分页查询参数
     *
     * @param params
     * @param i
     */
    protected List<Object> appendToParams(List<Object> params, long i) {
        ArrayList<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

    protected List<Object> buildParams(List<Map<String, Object>> mapList, StringBuilder sqlBuilder) {
        List<Object> insertParams = new ArrayList<>();
        int listIndex = 0;
        for (Map<String, Object> map : mapList) {
            if (listIndex > 0) {
                sqlBuilder.append(", (");
            } else {
                sqlBuilder.append(" (");
            }
            // 多一个同步时间字段
            int size = map.values().size() + 1;
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
            listIndex++;
        }
        return insertParams;
    }

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
        sqlBuilder.append(", sync_time ) VALUES ");
        return sqlBuilder;
    }

    protected class Pair {
        long companyId;
        long supplierId;

        public Pair(long companyId, long supplierId) {
            this.companyId = companyId;
            this.supplierId = supplierId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Pair pair = (Pair) o;

            if (companyId != pair.companyId) return false;
            return supplierId == pair.supplierId;

        }

        @Override
        public int hashCode() {
            int result = (int) (companyId ^ (companyId >>> 32));
            result = 31 * result + (int) (supplierId ^ (supplierId >>> 32));
            return result;
        }
    }


}
