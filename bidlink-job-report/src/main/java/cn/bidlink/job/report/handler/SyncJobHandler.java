package cn.bidlink.job.report.handler;

import com.xxl.job.core.handler.IJobHandler;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.util.Date;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
public abstract class SyncJobHandler extends IJobHandler{

    @Autowired
    @Qualifier("reportDataSource")
    protected DataSource reportDataSource;

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
}
