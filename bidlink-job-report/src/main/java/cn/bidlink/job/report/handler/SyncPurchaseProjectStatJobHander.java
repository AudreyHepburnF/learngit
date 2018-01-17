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
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购项目报表统计
 * @Date 2018/1/5
 */
@Service
@JobHander("syncPurchaseProjectStatJobHander")
public class SyncPurchaseProjectStatJobHander extends SyncJobHandler /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseProjectStatJobHander.class);

    private String COMPANY_ID = "company_id";
    private String COMP_ID = "comp_id";
    private String PROJECT_ID = "project_id";
    private String NOTICE_CREATE_TIME = "notice_create_time";
    private String SUPPLIER_BID_STATUS = "supplier_bid_status";
    private String INVITE_FLAG = "invite_flag";
    private String SUBMIT_PRICE_SUM = "submit_price_sum";
    private String NO_SUBMIT_PRICE_SUM = "no_submit_price_sum";
    private String INVITE_PRICE = "invite_price";
    private String NO_INVITE_PRICE = "no_invite_price";
    private String CHANGE_SUM = "change_sum";
    private String NOT_APPROVE_SUM = "not_approve_sum";
    private String BID_STATUS = "bid_status";
    private String DIRECTORY_ID = "directory_id";
    private String DEAL_STATUS = "deal_status";
    private String DEAL_TOTAL_PRICE = "deal_total_price";
    private String SAVING_TOTAL_PRICE = "saving_total_price";
    private String SAVE_TOTAL_RATIO = "save_total_ratio";
    private String SAVE_INVITED_RATIO = "save_invited_ratio";

    @Override
    protected String getTableName() {
        return "purchase_project_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购项目报表统计开始");
        syncPurchaseProject();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步采购项目报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncPurchaseProject() {
        Date lastSyncTime = getLastSyncTime();
        logger.info("同步采购项目报表统计 lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        String countSql = "SELECT\n" +
                "\tCOUNT(1)\n" +
                "FROM\n" +
                "\tbmpfjz_project bp\n" +
                "LEFT JOIN bmpfjz_project_ext bpe ON bp.comp_id = bpe.comp_id\n" +
                "AND bp.id = bpe.id\n" +
                "WHERE\n" +
                "\tcreate_time > ?\n";
        String querySql = "SELECT\n" +
                "\tbp.id AS project_id,\n" +
                "\tbp.comp_id AS company_id,\n" +
                "\tbp.comp_name AS company_name,\n" +
                "\tbp.user_id,\n" +
                "\tbp.user_name AS project_user_name,\n" +
                "\tbp.project_type,\n" +
                "\tbp.instance_id,\n" +
                "\tbp.instance_status,\n" +
                "\tbp.sub_instance_id,\n" +
                "\tbp.sub_instance_finished,\n" +
                "\tbp.project_status,\n" +
                "\tbp.approve_status,\n" +
                "\tbp. NAME AS project_name,\n" +
                "\tbp. CODE AS project_code,\n" +
                "\tbp.source_type AS project_source_type,\n" +
                "\tbp.source_name,\n" +
                "\tbp.open_bid_flag,\n" +
                "\tbp.bid_sheet_template_id,\n" +
                "\tbp.create_time AS project_create_time,\n" +
                "\tbp.open_bid_time,\n" +
                "\tbp.update_time,\n" +
                "\tbp.operator,\n" +
                "\tbp.reserve,\n" +
                "\tbpe.deal_total_price,\n" +
                "\tbpe.bid_stop_time AS notice_end_time,\n" +
                "\tbpe.publish_bid_result_time AS purchase_result_publish_time,\n" +
                "\tbpe.saving_total_price\n" +
                "\tFROM\n" +
                "\tbmpfjz_project bp\n" +
                "LEFT JOIN bmpfjz_project_ext bpe ON bp.comp_id = bpe.comp_id\n" +
                "AND bp.id = bpe.id\n" +
                "WHERE\n" +
                "\tcreate_time > ?\n" +
                "LIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }


    /**
     * 同步插入已完成采购项目
     *
     * @param dataSource
     * @param countSql
     * @param querySql
     * @param params
     */
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
                // 2.添加未报价和已报价的供应商数量
                appendSubmitPriceSum(mapList);
                // 3.添加变更次数
                appendChangeSum(mapList);
                // 4.添加审批未通过的次数
                appendNotApprovalSum(mapList);
                // 5.添加采购项目的成交情况
                appendDealStatus(mapList);
                // 6.添加全部节资率
                appendSavingTotalRatio(mapList);
                // 7.添加邀请节资率
                appendSavingInvitedRatio(mapList);

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
     * 添加邀请节资率
     *
     * @param mapList
     */
    private void appendSavingInvitedRatio(List<Map<String, Object>> mapList) {
        // 获取当前公司项目id
        Map<Long, List<Long>> compProjectIdList = new HashMap<>();
        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            List<Long> projectIds = compProjectIdList.get(companyId);
            if (CollectionUtils.isEmpty(projectIds)) {
                ArrayList<Long> pIds = new ArrayList<>();
                pIds.add((Long) map.get(PROJECT_ID));
                compProjectIdList.put((Long) companyId, pIds);
            } else {
                projectIds.add((Long) map.get(PROJECT_ID));
            }
        }

        // 邀请节资率
        final HashMap<String, Object> savingInviteRatioMap = new HashMap<>();
        for (Map.Entry<Long, List<Long>> compPIdEntry : compProjectIdList.entrySet()) {
            Long companyId = compPIdEntry.getKey();
            List<Long> projectIds = compPIdEntry.getValue();
            StringBuilder projectIdsSqlBuilder = new StringBuilder();
            // 拼接项目id
            projectIdsSqlBuilder.append(" ( ");
            long columnIndex = 0;
            for (Long projectId : projectIds) {
                if (columnIndex > 0) {
                    projectIdsSqlBuilder.append(",").append(projectId);
                } else {
                    projectIdsSqlBuilder.append(projectId);
                }
                columnIndex++;
            }
            projectIdsSqlBuilder.append(" ) ");
            String querySavingInvitedRatio = "SELECT\n" +
                    "\tp.id AS project_id, p.comp_id AS company_id,\n" +
                    "\tifnull(((sum(b.avg_price * b.deal_amount) - c.deal_total_price) / sum(b.avg_price * b.deal_amount)) * 100,0.00) AS save_invited_ratio\n" +
                    "FROM\n" +
                    "\tbmpfjz_project p\n" +
                    "LEFT JOIN (\n" +
                    "\tSELECT\n" +
                    "\t\ta_price.project_id AS project_id,\n" +
                    "\t\ta_price.comp_id AS comp_id,\n" +
                    "\t\ta_price.project_item_id AS project_item_id,\n" +
                    "\t\ta_price.avg_price AS avg_price,\n" +
                    "\t\ta_amount.deal_amount AS deal_amount\n" +
                    "\tFROM\n" +
                    "\t\t(\n" +
                    "\t\t\tSELECT\n" +
                    "\t\t\t\tIFNULL(AVG(a.bid_price), 0) AS avg_price,\n" +
                    "\t\t\t\ta.project_id,\n" +
                    "\t\t\t\ta.comp_id,\n" +
                    "\t\t\t\ta.project_item_id\n" +
                    "\t\t\tFROM\n" +
                    "\t\t\t\tbmpfjz_supplier_project_item_bid a\n" +
                    "\t\t\tINNER JOIN bmpfjz_supplier_project_bid ab ON ab.id = a.supplier_project_bid_id\n" +
                    "\t\t\tAND ab.comp_id = a.comp_id\n" +
                    "\t\t\tWHERE\n" +
                    "\t\t\t\tab.invite_flag = 1\n" +
                    "\t\t\tAND a.bid_status IN (3, 4)\n" +
                    "\t\t\tAND a.comp_id = " + companyId + "\n" +
                    "\t\t\tAND a.project_id in " + projectIdsSqlBuilder + "\n" +
                    "\t\t\tGROUP BY\n" +
                    "\t\t\t\ta.project_id,\n" +
                    "\t\t\t\ta.project_item_id\n" +
                    "\t\t) a_price\n" +
                    "\tINNER JOIN (\n" +
                    "\t\tSELECT\n" +
                    "\t\t\tsum(a.deal_amount) AS deal_amount,\n" +
                    "\t\t\ta.project_id,\n" +
                    "\t\t\ta.comp_id,\n" +
                    "\t\t\ta.project_item_id\n" +
                    "\t\tFROM\n" +
                    "\t\t\tbmpfjz_supplier_project_item_bid a\n" +
                    "\t\tINNER JOIN bmpfjz_supplier_project_bid ab ON ab.id = a.supplier_project_bid_id\n" +
                    "\t\tAND ab.comp_id = a.comp_id\n" +
                    "\t\tWHERE\n" +
                    "\t\t\ta.bid_status = 3\n" +
                    "\t\tAND ab.invite_flag = 1\n" +
                    "\t\tAND a.comp_id = " + companyId + "\n" +
                    "\t\tAND a.project_id in " + projectIdsSqlBuilder + "\n" +
                    "\t\tGROUP BY\n" +
                    "\t\t\ta.project_id,\n" +
                    "\t\t\ta.project_item_id\n" +
                    "\t) a_amount ON a_price.project_id = a_amount.project_id\n" +
                    "\tAND a_price.comp_id = a_amount.comp_id\n" +
                    "\tAND a_price.project_item_id = a_amount.project_item_id\n" +
                    ") b ON p.id = b.project_id\n" +
                    "AND p.comp_id = b.comp_id\n" +
                    "LEFT JOIN (\n" +
                    "\tSELECT\n" +
                    "\t\tsum(a.deal_amount * a.deal_price) AS deal_total_price,\n" +
                    "\t\ta.project_id,\n" +
                    "\t\ta.comp_id\n" +
                    "\tFROM\n" +
                    "\t\tbmpfjz_supplier_project_item_bid a\n" +
                    "\tINNER JOIN bmpfjz_supplier_project_bid ab ON ab.id = a.supplier_project_bid_id\n" +
                    "\tAND ab.comp_id = a.comp_id\n" +
                    "\tWHERE\n" +
                    "\t\ta.bid_status = 3\n" +
                    "\tAND ab.invite_flag = 1\n" +
                    "\tAND a.comp_id = " + companyId + "\n" +
                    "\tAND a.project_id in " + projectIdsSqlBuilder + "\n" +
                    "\tGROUP BY\n" +
                    "\t\ta.project_id\n" +
                    ") c ON p.id = c.project_id\n" +
                    "AND p.comp_id = c.comp_id\n" +
                    "WHERE\n" +
                    "\tp.project_status IN (8, 9, 10)\n" +
                    "AND p.comp_id = " + companyId + "\n" +
                    "AND p.id in " + projectIdsSqlBuilder + "\n" +
                    "GROUP BY\n" +
                    "\tp.id,\n" +
                    "\tp.comp_id\n" +
                    "\n";

            // 查询邀请节资率
            DBUtil.query(ycDataSource, querySavingInvitedRatio, null, new DBUtil.ResultSetCallback<Void>() {
                @Override
                public Void execute(ResultSet resultSet) throws SQLException {
                    while (resultSet.next()) {
                        long companyId = resultSet.getLong(COMPANY_ID);
                        long projectId = resultSet.getLong(PROJECT_ID);
                        String key = projectId + "_" + companyId;
                        savingInviteRatioMap.put(key, resultSet.getObject(SAVE_INVITED_RATIO));
                    }
                    return null;
                }
            });
        }

        // 添加邀请节资率
        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            BigDecimal invitedSavingRation = new BigDecimal(savingInviteRatioMap.get(key) == null ? "0.00" : savingInviteRatioMap.get(key).toString());
            NumberFormat invitedRatioFormat = NumberFormat.getNumberInstance();
            map.put(SAVE_INVITED_RATIO, invitedRatioFormat.format(invitedSavingRation.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue()) + "%");
        }
    }

    /**
     * 计算全部节资率
     *
     * @param mapList
     */
    private void appendSavingTotalRatio(List<Map<String, Object>> mapList) {
        for (Map<String, Object> map : mapList) {
            // 总中标金额
            BigDecimal total = (BigDecimal) map.get(DEAL_TOTAL_PRICE);
            total = (total == null ? BigDecimal.ZERO : total);
            // 总节约额
            BigDecimal savingTotal = ((BigDecimal) map.get(SAVING_TOTAL_PRICE));
            savingTotal = (savingTotal == null ? BigDecimal.ZERO : savingTotal);
            BigDecimal avgAvgDealPrice = total.add(savingTotal);
            BigDecimal zero = new BigDecimal(0);
            String str = "";
            if (avgAvgDealPrice == null || avgAvgDealPrice.compareTo(zero) == 0 || savingTotal == null || BigDecimal.ZERO.compareTo(savingTotal) == 0) {
                map.put(SAVE_TOTAL_RATIO, "0%");
            } else {
                BigDecimal bd = new BigDecimal(100);
                BigDecimal savingRatio = savingTotal.divide(avgAvgDealPrice, 4, BigDecimal.ROUND_HALF_UP).multiply(bd);
                double savingTotalRatio = savingRatio.doubleValue();
                NumberFormat number = NumberFormat.getNumberInstance();
                str = number.format(savingTotalRatio) + "%";
                map.put(SAVE_TOTAL_RATIO, str);
            }
        }
    }

    /**
     * 添加采购项目成交情况
     *
     * @param mapList
     */
    private void appendDealStatus(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT bid_status,directory_id,project_id,comp_id AS company_id FROM bmpfjz_supplier_project_item_bid");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));

        // 采购项目 供应商报价信息(招标状态和采购品id)
        Map<String, List<Map<String, Object>>> bidStatusMapList = DBUtil.query(ycDataSource, sqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, List<Map<String, Object>>>>() {
            @Override
            public Map<String, List<Map<String, Object>>> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, List<Map<String, Object>>> bidStatusMap = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long projectId = resultSet.getLong(PROJECT_ID);
                    String key = projectId + "_" + companyId;
                    List<Map<String, Object>> bidStatusList = bidStatusMap.get(key);
                    if (CollectionUtils.isEmpty(bidStatusList)) {
                        ArrayList<Map<String, Object>> list = new ArrayList<>();
                        HashMap<String, Object> map = new HashMap<>();
                        map.put(BID_STATUS, resultSet.getLong(BID_STATUS));
                        map.put(DIRECTORY_ID, resultSet.getLong(DIRECTORY_ID));
                        list.add(map);
                        bidStatusMap.put(key, list);
                    } else {
                        HashMap<String, Object> map = new HashMap<>();
                        map.put(BID_STATUS, resultSet.getLong(BID_STATUS));
                        map.put(DIRECTORY_ID, resultSet.getLong(DIRECTORY_ID));
                        bidStatusList.add(map);
                    }
                }
                return bidStatusMap;
            }
        });

        // 采购项目成交情况
        HashMap<String, Object> bidStatusMap = new HashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> bidStatusMapListEntry : bidStatusMapList.entrySet()) {
            // 计算成交情况
            String dealStatus = getDealstatus(bidStatusMapListEntry.getValue());
            bidStatusMap.put(bidStatusMapListEntry.getKey(), dealStatus);
        }

        // 添加采购项目成交情况
        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            Object dealStatus = bidStatusMap.get(key);
            if (null == dealStatus) {
                map.put(DEAL_STATUS, "未成交");
            } else {
                map.put(DEAL_STATUS, dealStatus);
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
            map.put(NOT_APPROVE_SUM, notApproveMap.get(key));
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
                    String key = projectId + "_" + companyId;
                    map.put(key, resultSet.getObject(CHANGE_SUM));
                }
                return map;
            }
        });

        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            map.put(CHANGE_SUM, changeSumMap.get(key));
        }

    }


    /**
     * 添加已报价和未报价的供应商数量
     *
     * @param mapList
     */
    private void appendSubmitPriceSum(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT supplier_bid_status,invite_flag,project_id,comp_id AS company_id FROM bmpfjz_supplier_project_bid");
        sqlBuilder.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));

        // 采购项目供应商报价信息(supplier_bid_status,invite_flag)
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

        // 采购项目已报价和未报价数量
        HashMap<String, String> submitPriceSumMap = new HashMap<>();
        for (Map.Entry<String, List<Map<String, Object>>> supplierProjectBidListEntry : supplierProjectBidListMap.entrySet()) {
            // 计算已报价和未报价供应商数量
            HashMap<String, Integer> priceMap = calculateBidPrice(supplierProjectBidListEntry.getValue());

            Integer submitPriceSum = priceMap.get(SUBMIT_PRICE_SUM);
            Integer noSubmitPriceSum = priceMap.get(NO_SUBMIT_PRICE_SUM);
            Integer invitePrice = priceMap.get(INVITE_PRICE);
            Integer noInvitePrice = priceMap.get(NO_INVITE_PRICE);
            String key = supplierProjectBidListEntry.getKey();
            String value = submitPriceSum + "," + noSubmitPriceSum + "," + invitePrice + "," + noInvitePrice;
            submitPriceSumMap.put(key, value);
        }

        // 添加已报价和未报价 邀请报价和未邀请报价供应商数量
        for (Map<String, Object> map : mapList) {
            Object companyId = map.get(COMPANY_ID);
            Object projectId = map.get(PROJECT_ID);
            String key = projectId + "_" + companyId;
            String value = submitPriceSumMap.get(key);
            if (value != null) {
                String[] priceArr = value.split(",");
                map.put(SUBMIT_PRICE_SUM, Integer.parseInt(priceArr[0]));
                map.put(NO_SUBMIT_PRICE_SUM, Integer.parseInt(priceArr[1]));
                map.put(INVITE_PRICE, Integer.parseInt(priceArr[2]));
                map.put(NO_INVITE_PRICE, Integer.parseInt(priceArr[3]));
            } else {
                map.put(SUBMIT_PRICE_SUM, 0);
                map.put(NO_SUBMIT_PRICE_SUM, 0);
                map.put(INVITE_PRICE, 0);
                map.put(NO_INVITE_PRICE, 0);
            }
        }
    }

    /**
     * 添加公告发布时间
     *
     * @param mapList
     */
    private void appendBulletinCreateTime(List<Map<String, Object>> mapList) {
        StringBuilder bulletinCreateTimeSql = new StringBuilder();
        bulletinCreateTimeSql.append("SELECT  project_id,comp_id AS company_id,create_time AS notice_create_time FROM bmpfjz_bulletin");
        bulletinCreateTimeSql.append(conditionSqlBuilder(mapList, COMP_ID, PROJECT_ID));

        Map<String, Object> createTimeMap = DBUtil.query(ycDataSource, bulletinCreateTimeSql.toString(), null, new DBUtil.ResultSetCallback<Map<String, Object>>() {
            @Override
            public Map<String, Object> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    long companyId = resultSet.getLong(COMPANY_ID);
                    long projectId = resultSet.getLong(PROJECT_ID);
                    String key = projectId + "_" + companyId;
                    Object createTime = map.get(key);
                    // 取第一条时间作为公告发布时间
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
     * @param companyIdColumnName 查询条件公司字段名称
     * @param projectIdColumnName 查询条件项目字段名称
     * @return
     */
    private StringBuilder conditionSqlBuilder(List<Map<String, Object>> mapList, String companyIdColumnName, String projectIdColumnName) {
        StringBuilder conditionSql = new StringBuilder();
        HashMap<String, Object> compIdAndProjectIdMap = new HashMap<>();
        for (Map<String, Object> mapAttr : mapList) {
            Object projectId = mapAttr.get(PROJECT_ID);
            Object companyId = mapAttr.get(COMPANY_ID);
            String key = projectId + "_" + companyId;
            compIdAndProjectIdMap.put(key, projectId);
        }

        conditionSql.append(" WHERE (");
        int columnIndex = 0;
        for (String key : compIdAndProjectIdMap.keySet()) {
            String projectId = key.split("_")[0];
            String companyId = key.split("_")[1];
            if (columnIndex > 0) {
                conditionSql.append(" OR ( ").append(companyIdColumnName).append(" = ").append(companyId)
                        .append(" AND ").append(projectIdColumnName).append(" = ").append(projectId).append(")");
            } else {
                conditionSql.append(" ( ").append(companyIdColumnName).append(" = ").append(companyId)
                        .append(" AND ").append(projectIdColumnName).append(" = ").append(projectId).append(")");

            }
            columnIndex++;
        }
        conditionSql.append(")");
        return conditionSql;
    }


    /**
     * 计算未报价和已报价的供应商数量
     *
     * @param supplierProjectBidList
     * @return
     */
    public HashMap<String, Integer> calculateBidPrice(List<Map<String, Object>> supplierProjectBidList) {
        HashMap<String, Integer> hm = new HashMap<>();
        int submitPriceSum = 0;
        int noSubmitPriceSum = 0;
        // 邀请报价
        int invitePrice = 0;
        // 非邀请报价
        int noInvitePrice = 0;
        for (Map<String, Object> itemMap : supplierProjectBidList) {
            if (null != itemMap.get(SUPPLIER_BID_STATUS) && ((Integer) itemMap.get(SUPPLIER_BID_STATUS) == 1
                    || (Integer) itemMap.get(SUPPLIER_BID_STATUS) == 0 || (Integer) itemMap.get(SUPPLIER_BID_STATUS) == 4)) {
                noSubmitPriceSum++;
            } else {
                if (null == itemMap.get(SUPPLIER_BID_STATUS)
                        || ((Integer) itemMap.get(SUPPLIER_BID_STATUS) != 4 && (Integer) itemMap.get(SUPPLIER_BID_STATUS) != 8)) {
                    submitPriceSum++;
                }
            }
            if (null != itemMap.get(INVITE_FLAG) && ((Integer) itemMap.get(INVITE_FLAG) == 1)) {
                invitePrice++;
            } else if (null != itemMap.get(INVITE_FLAG) && (Integer) itemMap.get(INVITE_FLAG) == 2) {
                noInvitePrice++;
            }
        }
        hm.put(SUBMIT_PRICE_SUM, submitPriceSum);
        hm.put(NO_SUBMIT_PRICE_SUM, noSubmitPriceSum);
        hm.put(INVITE_PRICE, invitePrice);
        hm.put(NO_INVITE_PRICE, noInvitePrice);
        return hm;

    }

    /**
     * 计算成交情况
     *
     * @param supplierProjectItemBid_list
     * @return
     */
    public String getDealstatus(List<Map<String, Object>> supplierProjectItemBid_list) {
        HashMap<Long, Integer> dealhm = new HashMap<>();
        HashMap<Long, Integer> hm = new HashMap<>();
        for (Map<String, Object> item : supplierProjectItemBid_list) {
            if (null != item.get(BID_STATUS) && (Long) item.get(BID_STATUS) == 3) {
                dealhm.put((Long) item.get(DIRECTORY_ID), 1);
            }
            hm.put((Long) item.get(DIRECTORY_ID), 1);
        }
        if (dealhm.size() == hm.size()) {
            return "全部成交";
        } else if (dealhm.size() < hm.size() && dealhm.size() > 0) {
            return "部分成交";
        } else if (dealhm.size() == 0) {
            return "未成交";
        } else {
            return "情况不明";
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
