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
 * @description:同步进行中采购项目报表统计
 * @Date 2018/1/5
 */
@Service
@JobHander("syncPurchaseProjectUnderwayStatJobHander")
public class SyncPurchaseProjectUnderwayStatJobHander extends SyncJobHandler /*implements InitializingBean*/{
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseProjectUnderwayStatJobHander.class);

    private String COMPANY_ID = "company_id";
    private String PROJECT_ID = "project_id";
    private String NOTICE_CREATE_TIME = "notice_create_time";
    private String ID = "id";
    private String COMP_ID = "comp_id";
    private String NOTICE_END_TIME = "notice_end_time";
    private String PURCHASE_RESULT_PUBLISH_TIME = "purchase_result_publish_time";
    private String SUPPLIER_BID_STATUS = "supplier_bid_status";
    private String INVITE_FLAG = "invite_flag";
    private String SUBMIT_PRICE_SUM = "submit_price_sum";
    private String NO_SUBMIT_PRICE_SUM = "no_submit_price_sum";
    private String INVITE_PRICE = "invite_price";
    private String NO_INVITE_PRICE = "no_invite_price";
    private String CHANGE_SUM = "change_sum";
    private String NOT_APPROVE_SUM = "not_approve_sum";

    @Override
    protected String getTableName() {
        return "purchase_project_underway_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步进行中采购项目报表统计开始");
        syncUnderwayPurchaseProject();
        // 记录同步时间
//        updateSyncLastTime();
        logger.info("同步进行中采购项目报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncUnderwayPurchaseProject() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步进行中采购项目报表统计 lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project\n" +
                "WHERE\n" +
                "\tcreate_time > ?";
        String querySql = "SELECT\n" +
                "\tid AS project_id,\n" +
                "\tcomp_id AS company_id,\n" +
                "\tcomp_name AS company_name,\n" +
                "\tuser_id,\n" +
                "\tuser_name AS project_user_name,\n" +
                "\tproject_type,\n" +
                "\tinstance_id,\n" +
                "\tinstance_status,\n" +
                "\tsub_instance_id,\n" +
                "\tsub_instance_finished,\n" +
                "\tproject_status,\n" +
                "\tapprove_status,\n" +
                "\tNAME AS project_name,\n" +
                "\tCODE AS project_code,\n" +
                "\tsource_type AS project_source_type,\n" +
                "\tsource_name,\n" +
                "\topen_bid_flag,\n" +
                "\tbid_sheet_template_id,\n" +
                "\tcreate_time AS project_create_time,\n" +
                "\topen_bid_time,\n" +
                "\tupdate_time,\n" +
                "\toperator,\n" +
                "\treserve\n" +
                "FROM\n" +
                "\tbmpfjz_project" +
                "\tWHERE create_time > ?" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
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
                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());

                // 1.添加公告发布时间
                appendBulletinCreateTime(mapList);
                // 2.添加投标截止时间和招标结果时间
                appendBidStopAndPublishTime(mapList);
                // 3.添加已报价的供应商数量
                appendSumbitPriceSum(mapList);
                // 4.添加变更次数
                appendChangeSum(mapList);
                // 5.添加审批未通过的次数
                appendNotApprovalSum(mapList);

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
     * 添加审批未通过的次数
     *
     * @param mapList
     */
    private void appendNotApprovalSum(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT COUNT(1) AS not_approve_sum,project_id,company_id\tFROM\tapproving\t");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMPANY_ID, PROJECT_ID));
        sqlBuilder.append("AND approve_status = 2\tAND approve_result = 2\tAND module = 3\tGROUP BY\tproject_id,company_id");

        Map<String, Object> notApproveMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, Object>>() {
            @Override
            public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long projectId = resultSet.getLong(PROJECT_ID);
                    long companyId = resultSet.getLong(COMPANY_ID);
                    String key = projectId + "_" + companyId;
                    map.put(key, resultSet.getObject(NOT_APPROVE_SUM));
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            map.put(NOT_APPROVE_SUM,notApproveMap.get(key));
        }
    }

    /**
     * 添加变更次数
     *
     * @param mapList
     */
    private void appendChangeSum(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT\tcount(1) AS change_sum,project_id ,comp_id AS company_id\tFROM\tbmpfjz_project_change_history");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));
        sqlBuilder.append("GROUP BY\tproject_id,company_id");

        Map<String, Object> changeSumMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, Object>>() {
            @Override
            public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long projectId = resultSet.getLong(PROJECT_ID);
                    String key = projectId + "_" + companyId ;
                    map.put(key, resultSet.getObject(CHANGE_SUM));
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            map.put(CHANGE_SUM,changeSumMap.get(key));
        }

    }


    /**
     * 添加已报价的供应商数量
     *
     * @param mapList
     */
    private void appendSumbitPriceSum(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT supplier_bid_status,invite_flag,project_id,comp_id AS company_id FROM bmpfjz_supplier_project_bid");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));

        Map<String, List<Map<String, Object>>> supplierProjectBidListMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, List<Map<String, Object>>>>() {
            @Override
            public Map<String, List<Map<String, Object>>> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, List<Map<String, Object>>> map = new HashMap<>();
                while (resultSet.next()) {
                    long projectId = resultSet.getLong(PROJECT_ID);
                    long companyId = resultSet.getLong(COMPANY_ID);
                    String key = projectId + "_" + companyId;
                    List<Map<String, Object>> supplierProjectBidList = map.get(key);
                    if (CollectionUtils.isEmpty(supplierProjectBidList)) {
                        ArrayList<Map<String, Object>> list = new ArrayList<>();
                        HashMap<String, Object> attrMap = new HashMap<>();
                        attrMap.put(SUPPLIER_BID_STATUS, resultSet.getObject(SUPPLIER_BID_STATUS));
                        attrMap.put(INVITE_FLAG, resultSet.getObject(INVITE_FLAG));
                        list.add(attrMap);
                        map.put(key, list);
                    } else {
                        HashMap<String, Object> attrMap = new HashMap<>();
                        attrMap.put(SUPPLIER_BID_STATUS, resultSet.getObject(SUPPLIER_BID_STATUS));
                        attrMap.put(INVITE_FLAG, resultSet.getObject(INVITE_FLAG));
                        supplierProjectBidList.add(attrMap);
                    }
                }
                return map;
            }
        });

        HashMap<String, Object> submitPriceSumMap = new HashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> supplierProjectBidListEntry : supplierProjectBidListMap.entrySet()) {
            Integer submitPriceSum = computerbidprice(supplierProjectBidListEntry.getValue()).get(SUBMIT_PRICE_SUM);
            String key = supplierProjectBidListEntry.getKey();
            submitPriceSumMap.put(key, submitPriceSum);
        }

        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            map.put(SUBMIT_PRICE_SUM, submitPriceSumMap.get(key));
        }

    }

    /**
     * 添加投标截止时间和招标结果时间
     *
     * @param mapList
     */
    private void appendBidStopAndPublishTime(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT bid_stop_time AS notice_end_time,publish_bid_result_time AS purchase_result_publish_time,id AS project_id,comp_id AS company_id FROM bmpfjz_project_ext");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, ID));

        Map<String, Object> timeMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, Object>>() {
            @Override
            public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long projectId = resultSet.getLong(PROJECT_ID);
                    long companyId = resultSet.getLong(COMPANY_ID);
                    String stopTimeKey = projectId + "_" + companyId + "stop";
                    String publishTimeKey = projectId + "_" + companyId + "publish";
                    map.put(stopTimeKey, resultSet.getObject(NOTICE_END_TIME));
                    map.put(publishTimeKey, resultSet.getObject(PURCHASE_RESULT_PUBLISH_TIME));
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            Object projectId = map.get(PROJECT_ID);
            Object companyId = map.get(COMPANY_ID);
            String stopKey = projectId + "_" + companyId + "stop";
            String publishTimeKey = projectId + "_" + companyId + "publish";
            map.put(NOTICE_END_TIME, timeMap.get(stopKey));
            map.put(PURCHASE_RESULT_PUBLISH_TIME, timeMap.get(publishTimeKey));
        }
    }

    /**
     * 添加公告发布时间
     *
     * @param mapList
     */
    private void appendBulletinCreateTime(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT  project_id,comp_id AS company_id,create_time AS notice_create_time FROM bmpfjz_bulletin");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));

        Map<String, Object> createTimeMap = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, Object>>() {
            @Override
            public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long projectId = resultSet.getLong(PROJECT_ID);
                    String key = projectId + "_" + companyId;
                    Object createTime = map.get(key);
                    if (createTime == null) {
                        map.put(key, resultSet.getObject(NOTICE_CREATE_TIME));
                    }
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            Object projectId = map.get(PROJECT_ID);
            Object companyId = map.get(COMPANY_ID);
            String key = projectId + "_" + companyId;
            map.put(NOTICE_CREATE_TIME, createTimeMap.get(key));
        }
    }

    /**
     * 拼接查询条件
     *
     * @param mapList
     * @return
     */
    private StringBuilder conditionSqlBuilder(List<Map<String, Object>> mapList, String companyIdColumnName, String projectIdColumnName) {
        StringBuilder sqlBuilder = new StringBuilder();
        HashMap<String, Object> conditionMap = new HashMap<>();
        for (Map<String, Object> mapAttr : mapList) {
            Object projectId = mapAttr.get(PROJECT_ID);
            Object companyId = mapAttr.get(COMPANY_ID);
            String key = projectId + "_" + companyId;
            conditionMap.put(key, projectId);
        }

        sqlBuilder.append(" WHERE (");
        int columnIndex = 0;
        for (String key : conditionMap.keySet()) {
            String projectId = key.split("_")[0];
            String companyId = key.split("_")[1];
            if (columnIndex > 0) {
                sqlBuilder.append(" OR ( ").append(companyIdColumnName).append(" = ").append(companyId)
                        .append(" AND ").append(projectIdColumnName).append(" = ").append(projectId).append(")");
            } else {
                sqlBuilder.append(" ( ").append(companyIdColumnName).append(" = ").append(companyId)
                        .append(" AND ").append(projectIdColumnName).append(" = ").append(projectId).append(")");

            }
            columnIndex++;
        }
        sqlBuilder.append(")");
        return sqlBuilder;
    }


    /**
     * 计算未报价给已报价的供应商数量
     *
     * @param supplierProjectBidList
     * @return
     */
    public HashMap<String, Integer> computerbidprice(List<Map<String, Object>> supplierProjectBidList) {
        HashMap<String, Integer> hm = new HashMap<String, Integer>();
        int submitpricesum = 0;
        int nosubmitpricesum = 0;
        // 邀请报价
        int inviteprice = 0;
        // 非邀请报价
        int noinviteprice = 0;
        for (Map<String, Object> itemMap : supplierProjectBidList) {
            if (null != itemMap.get(SUPPLIER_BID_STATUS) && ((Integer)itemMap.get(SUPPLIER_BID_STATUS) == 1
                    || (Integer)itemMap.get(SUPPLIER_BID_STATUS) == 0 || (Integer)itemMap.get(SUPPLIER_BID_STATUS) == 4)) {
                nosubmitpricesum++;
            } else {
                if (null == itemMap.get(SUPPLIER_BID_STATUS)
                        || ((Integer)itemMap.get(SUPPLIER_BID_STATUS) != 4 && (Integer)itemMap.get(SUPPLIER_BID_STATUS) != 8)) {
                    submitpricesum++;
                }
            }
            if (null != itemMap.get(INVITE_FLAG) && ((Integer)itemMap.get(INVITE_FLAG) == 1)) {
                inviteprice++;
            } else if (null != itemMap.get(INVITE_FLAG) && (Integer)itemMap.get(INVITE_FLAG) == 2) {
                noinviteprice++;
            }
        }
        hm.put(SUBMIT_PRICE_SUM, submitpricesum);
        hm.put(NO_SUBMIT_PRICE_SUM, nosubmitpricesum);
        hm.put(INVITE_PRICE, inviteprice);
        hm.put(NO_INVITE_PRICE, noinviteprice);
        return hm;

    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
