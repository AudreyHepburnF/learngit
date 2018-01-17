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
 * @author <a href="mailto:jiacaisu@ebnew.com">jiacaisu</a>
 * @version Ver 1.0
 * @description 供应商招标中标统计
 * @Date 2018/1/4
 */
@Service
@JobHander("syncSupplierBidingStatJobHandler")
public class SyncSupplierBidingStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncSupplierBidingStatJobHandler.class);
    private final String SUPPLIER_ID = "supplier_id";
    private final String COMPANY_ID = "company_id";
    private final String SUPPLIER_TYPE = "supplier_type";
    @Override
    protected String getTableName() {
        return "supplier_biding_stat";
    }


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        clearBidProcessStat();
        logger.info("同步供应商招标中标统计开始");
        //盘内供应商
        syncSupplierInner();
        //盘外供应商
      //  syncSupplierOuter();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步供应商招标中标统计结束");
        return ReturnT.SUCCESS;
    }

    private void clearBidProcessStat() {
        logger.info("清理供应商招标中标统计开始");
        clearTableData();
        logger.info("清理供应商招标中标统计开始");
    }

    private void syncSupplierInner() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步盘内供应商招标中标统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "\t\tSELECT\n" +
                "\t\tcount(1)\n" +
                "\t\tFROM\n" +
                "\t\t\tBID B\n" +
                "\t\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\t\tAND p.COMPANY_ID = b.COMPANY_ID ";


        String querySql = "\tSELECT\n" +
                "\t\t\tP.ID AS pid,\n" +
                "\t\t\tB.COMPANY_ID,\n" +
                "\t\t\tP.PROJECT_NAME,\n" +
                "\t\t\tB.BIDER_NAME AS SUPPLIER_NAME,\n" +
                "\t\t\tB.BIDER_ID AS SUPPLIER_ID,\n" +
                "\t\t\tB.BIDER_PRICE_UNE,\n" +
                "\t\t\tb.ID as bidId,\n" +
                "\t\t\tB.IS_BID_SUCCESS,\n" +
                "\t\t\tB.IS_ABANDON ,\n" +
                "\t\t\tB.IS_WITHDRAWBID ,\n" +
                "\t\t\tP.PROJECT_STATUS ,\n" +
                "\t\t\tP.CREATE_TIME,\n" +
                "\t\t\tB.BIDER_ID\n" +
                "\t\tFROM\n" +
                "\t\t\tBID B\n" +
                "\t\tLEFT JOIN PROJ_INTER_PROJECT P ON P.ID = B.PROJECT_ID\n" +
                "\t\tAND p.COMPANY_ID = b.COMPANY_ID " +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }






 /*   @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/

    @Override
    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                //判断当前供应商是否盘内供应商还是盘外供应商
                appendSupplierType(mapList);
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

    private void appendSupplierType(List<Map<String, Object>> mapList) {
        Set<Pair> pairs = new HashSet<>();
        for (Map<String, Object> map : mapList) {
            pairs.add(new Pair(((long) map.get(COMPANY_ID)), ((long) map.get(SUPPLIER_ID))));
        }

        StringBuffer querySupplierStatusSql = new StringBuffer("SELECT company_id,supplier_id, supplier_status FROM bsm_company_supplier_apply WHERE ");
        int count = 0;
        for (Pair pair : pairs) {
            if (count > 0) {
                querySupplierStatusSql.append(" OR ");
            }
            querySupplierStatusSql.append(" (company_id=")
                    .append(pair.companyId)
                    .append(" AND supplier_id=")
                    .append(pair.supplierId)
                    .append(") ");
            count++;
        }

        // 查询对应供应商的类型
        Map<String, String> supplierTypeMap = DBUtil.query(ycDataSource, querySupplierStatusSql.toString(), null, new DBUtil.ResultSetCallback<Map<String, String>>() {
            @Override
            public Map<String, String> execute(ResultSet resultSet) throws SQLException {
                Map<String, String> map = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong("company_id");
                    long supplierId = resultSet.getLong("supplier_id");
                    String supplierStatus = resultSet.getString("supplier_status");
                    String key = companyId + "_" + supplierId;
                    map.put(key, supplierStatus);
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            String key = map.get(COMPANY_ID) + "_" + map.get(SUPPLIER_ID);
            String supplierStatus = supplierTypeMap.get(key);
            //saas定义的主要状态不是4都是盘外供应商
            if("4".equals(supplierStatus)){
                map.put(SUPPLIER_TYPE, 0);
            }else{
                map.put(SUPPLIER_TYPE, 1);
            }

        }
    }
}
