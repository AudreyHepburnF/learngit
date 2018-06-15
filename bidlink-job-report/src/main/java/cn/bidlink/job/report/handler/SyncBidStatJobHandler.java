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
 * 招标概况统计
 * 注意：因为招标数据只有创建时间字段，且数据量较少，采用全量同步的方式
 *
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2017/12/25
 */
@Service
@JobHander("syncBidStatJobHandler")
public class SyncBidStatJobHandler extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncBidStatJobHandler.class);

    private final String SUPPLIER_ID = "supplier_id";
    private final String COMPANY_ID = "company_id";
    private final String SUPPLIER_TYPE = "supplier_type";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步招标流程项目概况统计开始");
        clearBidProcessStat();
        syncBidProcessStat();
        //记录同步时间
        updateSyncLastTime();
        logger.info("同步招标流程项目概况统计结束");
        return ReturnT.SUCCESS;
    }

    private void clearBidProcessStat() {
        logger.info("清理招标流程项目概况统计开始");
        clearTableData();
        logger.info("清理招标流程项目概况统计结束");
    }

    @Override
    protected String getTableName() {
        return "bid_stat";
    }

    /**
     * 由于招标数据只有create_time，所以每次同步是全量更新
     */
    private void syncBidProcessStat() {
        // 获取上次同步时间
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步报价供应商lastSyncTime：" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = " SELECT\n"
                          + "         count(1)\n"
                          + "      FROM\n"
                          + "         proj_inter_project t1\n"
                          + "      LEFT JOIN bid_decided t4 ON t1.COMPANY_ID = t4.COMPANY_ID\n"
                          + "      AND t1.id = t4.PROJECT_ID\n"
                          + "      LEFT JOIN bid t2 ON t1.COMPANY_ID = t2.COMPANY_ID\n"
                          + "      AND t1.id = t2.PROJECT_ID\n"
                          + "      LEFT JOIN bid_product t3 ON t2.COMPANY_ID = t3.COMPANY_ID\n"
                          + "      AND t2.id = t3.BID_ID\n"
                          + "      WHERE t1.PROJECT_STATUS <> 12";
//                          + "AND t1.CREATE_TIME > ?";


        String querySql = " SELECT\n"
                          + "   t1.project_amount_rmb,\n"
                          + "   t1.company_id,\n"
                          + "   t1.id as project_id,\n"
                          + "   t3.product_name,\n"
                          + "   t1.project_status,\n"
                          + "   t2.is_bid_success,\n"
                          + "   t2.is_withdrawbid,\n"
                          + "   t2.BIDER_ID AS supplier_id,\n"
                          + "   t2.BIDER_NAME AS supplier_name,\n"
                          + "   t2.bider_price_une,\n"
                          + "   t1.create_time,\n"
                          + "   t4.update_date,\n"
                          + "   t1.department_code\n"
                          + "      FROM\n"
                          + "         proj_inter_project t1\n"
                          + "      LEFT JOIN bid_decided t4 ON t1.COMPANY_ID = t4.COMPANY_ID\n"
                          + "      AND t1.id = t4.PROJECT_ID\n"
                          + "      LEFT JOIN bid t2 ON t1.COMPANY_ID = t2.COMPANY_ID\n"
                          + "      AND t1.id = t2.PROJECT_ID\n"
                          + "      LEFT JOIN bid_product t3 ON t2.COMPANY_ID = t3.COMPANY_ID\n"
                          + "      AND t2.id = t3.BID_ID\n"
                          + "      WHERE t1.PROJECT_STATUS <> 12\n"
//                          + "AND t1.CREATE_TIME > ?\n"
                          + "LIMIT ?,?";
        List<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);


    }

    @Override
    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                // 添加供应商类型
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
            if (map.get(SUPPLIER_ID) != null) {
                pairs.add(new Pair(((long) map.get(COMPANY_ID)), ((long) map.get(SUPPLIER_ID))));
            }
        }

        StringBuffer querySupplierStatusSql = new StringBuffer("SELECT company_id,supplier_id,IF (supplier_status = 1, '是', '否') AS supplier_type FROM bsm_company_supplier WHERE ");
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
                    String supplierType = resultSet.getString("supplier_type");
                    String key = companyId + "_" + supplierId;
                    map.put(key, supplierType);
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            String key = map.get(COMPANY_ID) + "_" + map.get(SUPPLIER_ID);
            String supplierType = supplierTypeMap.get(key);
            map.put(SUPPLIER_TYPE, supplierType);
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
