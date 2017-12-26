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
public abstract class SyncJobHandler extends IJobHandler{
    private Logger logger = LoggerFactory.getLogger(SyncJobHandler.class);

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

    protected Date getLastSyncTime() {
        String querySql = "SELECT sync_time FROM sync_report WHERE table_name = ?";
        List<Object> params = new ArrayList<>();
        params.add(getTableName());
        List<Map<String, Object>> mapList = DBUtil.query(reportDataSource, querySql, params);
        if (CollectionUtils.isEmpty(mapList)) {
            return SyncTimeUtil.GMT_TIME;
        } else {
            return (Date) mapList.get(0).get("sync_time");
        }
    }

    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, params);
                logger.debug("执行querySql : {}, params : {}，共{}条", querySql, params, mapList.size());
                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    StringBuilder sqlBuilder = sqlBuilder(mapList);
                    List<Object> insertParams = buildParams(mapList, sqlBuilder);
                    DBUtil.execute(reportDataSource, sqlBuilder.toString(), insertParams);
                }
            }
        }
    }

    private List<Object> buildParams(List<Map<String, Object>> mapList, StringBuilder sqlBuilder) {
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

    private StringBuilder sqlBuilder(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(getTableName()).append(" (");
        int columnIndex = 0;
        for (String columnName : mapList.get(0).keySet()) {
            if (columnIndex > 0) {
                sqlBuilder.append(columnName).append(", ");
            } else {
                sqlBuilder.append(columnName);
            }
            columnIndex++;
        }
        sqlBuilder.append(", sync_time ) VALUES ");
        return sqlBuilder;
    }
}
