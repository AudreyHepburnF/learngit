package cn.bidlink.job.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.springframework.util.StringUtils;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/8/25
 */
public class SyncTimeUtil {
    public static String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    public static String SYNC_TIME = "syncTime";

    public static Timestamp GMT_TIME = new Timestamp(0);

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

    public static String toDateString(Object propertyValue) {
        if (propertyValue instanceof java.util.Date) {
            return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
        } else {
            return String.valueOf(propertyValue);
        }
    }

    public static String currentDateToString() {
        return toDateString(getCurrentDate());
    }

    public static Date toStringDate(String propertyValue) {
        if (!StringUtils.isEmpty(propertyValue)) {
            return DateTime.parse(propertyValue, DateTimeFormat.forPattern(SyncTimeUtil.DATE_TIME_PATTERN)).toDate();
        }
        return null;
    }

    /**
     * 获取零点时间
     *
     * @return
     */
    public static Date getZeroTime() {
        DateTime dateTime = new DateTime();
        DateTime zeroTime = dateTime.minusHours(dateTime.getHourOfDay())
                .minusMinutes(dateTime.getMinuteOfHour())
                .minusSeconds(dateTime.getSecondOfMinute())
                .minusMillis(dateTime.getMillisOfSecond());
        return zeroTime.toDate();
    }

    /**
     * 获取明天
     * @return
     */
    public static Date getTomorrow(Date date){
        Calendar cal=Calendar.getInstance();
        cal.setTime(date);//当天开始时间
        cal.add(Calendar.DAY_OF_MONTH, 1);//当天月份天数加1
        return cal.getTime();
    }


    public static Long getZeroTimeLongValue() {
        DateTime dateTime = new DateTime();
        DateTime zeroTime = dateTime.minusHours(dateTime.getHourOfDay())
                .minusMinutes(dateTime.getMinuteOfHour())
                .minusSeconds(dateTime.getSecondOfMinute())
                .minusMillis(dateTime.getMillisOfSecond());
        return zeroTime.toDate().getTime();
    }

    public static Map<String, Object> handlerDate(Map<String, Object> result) {
        String jsonMap = JSON.toJSONString(result, (ValueFilter) (Object object, String propertyName, Object propertyValue) -> {
            if (propertyValue instanceof Date) {
                return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
            } else {
                return propertyValue;
            }
        });
        return JSON.parseObject(jsonMap, Map.class);
    }
}
