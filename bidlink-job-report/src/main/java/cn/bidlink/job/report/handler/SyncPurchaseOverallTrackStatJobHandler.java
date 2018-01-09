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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:jiacaisu@ebnew.com">jiacaisu</a>
 * @version Ver 1.0
 * @description 招标整体跟踪统计
 * @Date 2018/1/4
 */
@Service
@JobHander("syncPurchaseOverallTrackStatJobHandler")
public class SyncPurchaseOverallTrackStatJobHandler extends SyncJobHandler /*implements InitializingBean */{

    private Logger logger = LoggerFactory.getLogger(SyncPurchaseOverallTrackStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "purchase_overall_tracking_stat";
    }



    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();

        logger.info("同步招标整体跟踪统计开始");
        //进行中
        syncPurchaseOverallTracking();
        //已结束
        syncPurchaseOverallTrackEnd();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步招标整体跟踪统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseOverallTracking() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步招标整体跟踪进行中-统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tCOUNT(DISTINCT P.ID)\n" +
                "FROM\n" +
                "\tPROJ_INTER_PROJECT P\n" +
                "LEFT JOIN NOTICE_BID N ON N.PROJECT_ID = P.ID\n" +
                "AND n.PUBLISHER_ID = p.COMPANY_ID\n" +
                "WHERE\n" +
                "\tP.PROJECT_STATUS NOT IN (1, 9, 11, 12)\n" +
                "AND P.IS_PROJECT_DIRECTORY = 1";


        String querySql = "SELECT DISTINCT\n" +
                "\tP.ID AS ID,\n" +
                "\tP.PROJECT_NUMBER AS PROJECT_NUMBER,\n" +
                "\tP.PROJECT_NAME AS PROJECT_NAME,\n" +
                "\tP.PROJECT_STATUS AS PROJECT_STATUS,\n" +
                "\tP.CREATE_TIME AS CREATE_TIME,\n" +
                "\tP.PROJECT_TYPE AS PROJECT_TYPE,\n" +
                "\tP.CREATE_USER_NAME AS CREATE_USER_NAME,\n" +
                "\tP.TENDER_MODE AS TENDER_MODE,\n" +
                "\tP.KIND AS KIND,\n" +
                "\tp.COMPANY_ID,\n" +
                "\t1 as STAT_STATUS,\n"+
                "\tP.CREATER_NAME,\n" +
                "\tP.TENDER_NAMES,\n" +
                "\tP.TENDER_FORM,\n" +
                "\tN.BID_ENDTIME,\n" +
                "\tP.PROJECT_AMOUNT_RMB,\n" +
                "P.IS_PROJECT_DIRECTORY\n" +
                "FROM\n" +
                "\tPROJ_INTER_PROJECT P\n" +
                "LEFT JOIN NOTICE_BID N ON N.PROJECT_ID = P.ID\n" +
                "AND p.COMPANY_ID = n.PUBLISHER_ID\n" +
                "WHERE\n" +
                "\tP.PROJECT_STATUS NOT IN (1, 9, 11, 12)\n" +
                "AND P.IS_PROJECT_DIRECTORY = 1 " +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }

    private void   syncPurchaseOverallTrackEnd(){

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步招标整体跟踪已完成统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\t(\n" +
                "\t\tSELECT\n" +
                "\t\t\tCOUNT(DISTINCT P.ID)\n" +
                "\t\tFROM\n" +
                "\t\t\tPROJ_INTER_PROJECT P\n" +
                "\t\tLEFT JOIN NOTICE_BID N ON N.PROJECT_ID = P.ID\n" +
                "\t\tAND N.PUBLISHER_ID = p.COMPANY_ID\n" +
                "\t\tLEFT JOIN BID B ON B.PROJECT_ID = P.ID\n" +
                "\t\tAND b.COMPANY_ID = p.COMPANY_ID\n" +
                "\t\tWHERE\n" +
                "\t\t\tP.PROJECT_STATUS IN (9, 11, 12)\n" +
                "\t\tGROUP BY\n" +
                "\t\t\tP.PROJECT_NUMBER\n" +
                "\t) AS temp";


        String querySql = "SELECT DISTINCT\n" +
                "\tP.ID AS ID,\n" +
                "\tP.PROJECT_NUMBER AS PROJECT_NUMBER,\n" +
                "\tP.PROJECT_NAME AS PROJECT_NAME,\n" +
                "\tP.PROJECT_STATUS AS PROJECT_STATUS,\n" +
                "\tP.CREATE_TIME AS CREATE_TIME,\n" +
                "\tP.PROJECT_TYPE AS PROJECT_TYPE,\n" +
                "\tP.CREATE_USER_NAME AS CREATE_USER_NAME,\n" +
                "\tP.TENDER_MODE AS TENDER_MODE,\n" +
                "\tP.KIND AS KIND,\n" +
                "\tN.BID_ENDTIME AS BID_ENDTIME,\n" +
                "\tP.PROJECT_AMOUNT_RMB,\n" +
                "\t2 AS STAT_STATUS,\n" +
                "\tcount(B.id) AS BID_SUCCESS_COUNT,\n" +
                "\tp.COMPANY_ID,\n" +
                "\tp.CREATER_NAME,\n" +
                "\tp.TENDER_NAMES\n" +
                "FROM\n" +
                "\tPROJ_INTER_PROJECT P\n" +
                "LEFT JOIN NOTICE_BID N ON N.PROJECT_ID = P.ID\n" +
                "AND n.PUBLISHER_ID = p.COMPANY_ID\n" +
                "LEFT JOIN BID B ON B.PROJECT_ID = P.ID\n" +
                "AND b.COMPANY_ID = p.COMPANY_ID\n" +
                "WHERE\n" +
                "\tP.PROJECT_STATUS IN (9, 11, 12)\n" +
                "GROUP BY\n" +
                "\tP.PROJECT_NUMBER " +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }




  /*  @Override
    public void afterPropertiesSet() throws Exception {
        execute();
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
                    System.out.println(mapList);
                }
            }
        }
    }*/
}
