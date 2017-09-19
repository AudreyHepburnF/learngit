package cn.bidlink.job.common.utils;

import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/25
 */
public class SyncTimeUtil {
    public static String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static String SYNC_TIME = "syncTime";

    private static ThreadLocal<Timestamp> currentDate = new InheritableThreadLocal<>();

    public static void setCurrentDate() {
        currentDate.set(new Timestamp(new DateTime().getMillis()));
    }

    public static void setDate(Date date) {
        setDate(date.getTime());
    }

    public static void setDate(long timestamp) {
        currentDate.set(new Timestamp(timestamp));
    }


    public static Timestamp getCurrentDate() {
        return currentDate.get();
    }
}
