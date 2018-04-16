package cn.bidlink.job.business.handler;

import cn.bidlink.job.business.utils.AreaUtil;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步xieTong平台采购商(注意:可以替代 {@link SyncPurchaseDataJobHandler})
 * @Date 2017/11/29
 */
@Service
@JobHander(value = "syncXieTongPurchaseDataJobHandler")
public class SyncXieTongPurchaseDataJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncXieTongPurchaseDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("purchaseDataSource")
    private DataSource purchaseDataSource;

    @Autowired
    @Qualifier("tenderDataSource")
    private DataSource tenderDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

    @Value("${pageSize}")
    private int pageSize;

    private String ID                      = "id";
    private String COMPANY_ID              = "companyId";
    private String PURCHASE_TRADING_VOLUME = "purchaseTradingVolume";
    private String BID_TRADING_VOLUME      = "bidTradingVolume";
    private String TRADING_VOLUME          = "tradingVolume";
    private String LONG_TRADING_VOLUME     = "longTradingVolume";
    private String COMPANY_SITE_ALIAS      = "companySiteAlias";
    private String PURCHASE_PROJECT_COUNT  = "purchaseProjectCount";
    private String BID_PROJECT_COUNT       = "bidProjectCount";
    private String PROJECT_COUNT           = "projectCount";
    private String REGION                  = "region";
    private String AREA_STR                = "areaStr";
    private String AREA_STR_NOT_ANALYZED   = "areaStrNotAnalyzed";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步协同平台采购商开始");
        synPurchase();
        logger.info("同步协同平台采购商结束");
        return ReturnT.SUCCESS;
    }

    private void synPurchase() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.index", "cluster.type.purchase", null);
        logger.info("同步新平台采购商数据 lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + "\n"
                + ",syncTime:" + new DateTime(SyncTimeUtil.getCurrentDate()).toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncPurchaserDataService(lastSyncTime);
        // 同步没有开户信息采购商数据
        syncOldPurchaserDataService(lastSyncTime);
        syncPurchaserStatService(lastSyncTime);

    }

    private void syncOldPurchaserDataService(Timestamp lastSyncTime) {
        // 初始化后只需要更新
        syncOldUpdatedPurchaserData(lastSyncTime);
    }

    private void syncOldUpdatedPurchaserData(Timestamp lastSyncTime) {
            String countUpdatedPurchaseSql = "SELECT\n"
                    + "   count(1)\n"
                    + "FROM\n"
                    + "   t_reg_company trc\n"
                    + "   left join open_account oa ON trc.ID = oa.COMPANY_ID \n"
                    + "WHERE\n"
                    + "     trc.TYPE = 12\n"
                    + "AND oa.EXAMINE_STATUS = 2\n"
                    + "AND  trc.update_time >= ?"
                    + "AND trc.create_date < \"2017-06-27 14:51:13\"";
            String queryUpdatedPurchaseSql = "SELECT\n" +
                    "\ttrc.id,\n" +
                    "\ttrc.NAME AS purchaseName,\n" +
                    "\ttrc.NAME AS purchaseNameNotAnalyzed,\n" +
                    "\ttrc.WWW_STATION AS wwwStationAlias,\n" +
                    "\ttrc.INDUSTRY_STR AS industryStr,\n" +
                    "\ttrc.INDUSTRY_STR AS industryStrNotAnalyzed,\n" +
                    "\ttrc.INDUSTRY AS industry,\n" +
                    "\ttrc.ZONE_STR AS zoneStr,\n" +
                    "\ttrc.ZONE_STR AS zoneStrNotAnalyzed,\n" +
                    "\ttrc.COMP_TYPE_STR AS compTypeStr,\n" +
                    "\ttrc.company_site AS companySiteAlias \n" +
                    "FROM\n" +
                    "\tt_reg_company trc \n" +
                    "WHERE\n" +
                    "\ttrc.TYPE = 12 \n" +
                    "\tAND trc.create_date < \"2017-06-27 14:51:13\"" +
                    "\tAND  trc.update_time >= ?\n" +
                    "LIMIT ?, ?";
            ArrayList<Object> params = new ArrayList<>();
            params.add(lastSyncTime);
            doSyncPurchaserDataService(countUpdatedPurchaseSql, queryUpdatedPurchaseSql, params);
        }

    /**
     * 同步采购商参与项目和交易额统计
     * <p>
     * 每次统计覆盖之前的数据
     *
     * @param lastSyncTime
     */
    private void syncPurchaserStatService(Timestamp lastSyncTime) {
        logger.info("同步采购商项目和交易额统计开始");
        Properties properties = elasticClient.getProperties();
        int pageSizeToUse = 2 * pageSize;
        SearchResponse scrollResp = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.index"))
                .setTypes(properties.getProperty("cluster.type.purchase"))
                .setScroll(new TimeValue(60000))
                .setSize(pageSizeToUse)
                .get();

        int pageNumberToUse = 0;
        do {
            SearchHits hits = scrollResp.getHits();
            ArrayList<String> purchaserIds = new ArrayList<>();
            ArrayList<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit hit : hits.getHits()) {
                purchaserIds.add(((String) hit.getSource().get(ID)));
                resultFromEs.add(hit.getSource());
            }

            String purchaserIdToString = StringUtils.collectionToCommaDelimitedString(purchaserIds);
            // 添加采购交易额
            appendPurchaseTradingVolume(resultFromEs, purchaserIdToString);

            // 添加招标交易额
            appendBidTradingVolume(resultFromEs, purchaserIdToString);

            // 添加采购项目数量
            appendPurchaseProjectCount(resultFromEs, purchaserIdToString);

            // 添加招标项目数量
            appendBidProjectCount(resultFromEs, purchaserIdToString);

            batchInsert(resultFromEs);
            System.out.println("回滚后数据" + resultFromEs);
            scrollResp = elasticClient.getTransportClient().prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(60000))
                    .execute()
                    .actionGet();
            pageNumberToUse++;
        } while (scrollResp.getHits().getHits().length != 0);
        logger.info("同步采购商参与项目和交易统计结束");

    }

    private void syncPurchaserDataService(Timestamp lastSyncTime) {
        syncCreatePurchaserData(lastSyncTime);
        syncUpdatedPurchaserData(lastSyncTime);
    }

    private void syncCreatePurchaserData(Timestamp lastSyncTime) {
        String countCreatedPurchaseSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "   left join open_account oa ON trc.ID = oa.COMPANY_ID \n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND oa.EXAMINE_STATUS = 2\n"
                + "AND  trc.create_date >= ?";
        String queryCreatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.name AS purchaseNameNotAnalyzed,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.INDUSTRY_STR AS industryStrNotAnalyzed,\n"
                + "   trc.INDUSTRY_STR AS industryStr,\n"
                + "   trc.INDUSTRY AS industry,\n"
                + "   trc.ZONE_STR AS zoneStrNotAnalyzed,\n"
                + "   trc.ZONE_STR AS zoneStr,\n"
                + "   trc.COMP_TYPE_STR AS compTypeStr,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "   left join open_account oa ON trc.ID = oa.COMPANY_ID \n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND oa.EXAMINE_STATUS = 2\n"
                + "AND  trc.create_date >= ?\n"
                + "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaserDataService(countCreatedPurchaseSql, queryCreatedPurchaseSql, params);
    }

    private void syncUpdatedPurchaserData(Timestamp lastSyncTime) {
        String countUpdatedPurchaseSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "   left join open_account oa ON trc.ID = oa.COMPANY_ID \n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND oa.EXAMINE_STATUS = 2\n"
                + "AND  trc.update_time >= ?";
        String queryUpdatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.name AS purchaseNameNotAnalyzed,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.INDUSTRY_STR AS industryStr,\n"
                + "   trc.INDUSTRY_STR AS industryStrNotAnalyzed,\n"
                + "   trc.INDUSTRY AS industry,\n"
                + "   trc.ZONE_STR AS zoneStr,\n"
                + "   trc.ZONE_STR AS zoneStrNotAnalyzed,\n"
                + "   trc.COMP_TYPE_STR AS compTypeStr,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "   left join open_account oa ON trc.ID = oa.COMPANY_ID \n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND oa.EXAMINE_STATUS = 2\n"
                + "AND   trc.update_time >= ?\n"
                + "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaserDataService(countUpdatedPurchaseSql, queryUpdatedPurchaseSql, params);
    }

    /**
     * 同步数据到es中
     *
     * @param countSql 查询总条数sql
     * @param querySql 查询结果集sql
     * @param params   参数 lastSyncTime es中最后同步时间
     */
    private void doSyncPurchaserDataService(String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(centerDataSource, countSql, params);
        logger.debug("执行countSql: {}, params: {}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询的参数
                List<Object> paramsToUse = appendToParams(params, i);
                // 查询分页结果集
                List<Map<String, Object>> purchasers = DBUtil.query(centerDataSource, querySql, paramsToUse);
                logger.debug("执行querySql : {}, params: {},共{}条", querySql, paramsToUse, purchasers.size());
                // 采购商id
                HashSet<Long> purchaserIds = new HashSet<>();
                for (Map<String, Object> purchaser : purchasers) {
                    purchaserIds.add(((Long) purchaser.get(ID)));
                    // 添加同步时间
                    refresh(purchaser);
                }
                // 添加采购商区域信息
                appendPurchaseRegion(purchasers, purchaserIds);
                // 批量插入es中
                batchInsert(purchasers);
            }
        }
    }

    private void appendPurchaseTradingVolume(List<Map<String, Object>> purchases, String purchaserIdToString) {
        String queryPurchaserSqlTemplate = "SELECT\n" +
                "\tsum( ppe.deal_total_price ) AS purchaseTradingVolume,\n" +
                "\tpp.company_id AS companyId \n" +
                "FROM\n" +
                "\tpurchase_project pp\n" +
                "\tLEFT JOIN purchase_project_ext ppe ON pp.id = ppe.id \n" +
                "\tAND pp.company_id = ppe.company_id \n" +
                "\t\n" +
                "WHERE\n" +
                "\tpp.process_status IN ( 31, 40 )  \n" +
                "\tAND ppe.deal_total_price IS NOT NULL \n" +
                "\tAND pp.company_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tpp.company_id;";

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryPurchaseSql = String.format(queryPurchaserSqlTemplate, purchaserIdToString);
            // 根据采购商id查询交易额
            List<Map<String, Object>> purchaseTradingVolumeList = DBUtil.query(purchaseDataSource, queryPurchaseSql, null);
            HashMap<String, BigDecimal> purchaseAttributeMap = new HashMap<>();
            // list<Map>转换为map
            if (!CollectionUtils.isEmpty(purchaseTradingVolumeList)) {
                for (Map<String, Object> map : purchaseTradingVolumeList) {
                    purchaseAttributeMap.put(String.valueOf(map.get(COMPANY_ID)), ((BigDecimal) map.get(PURCHASE_TRADING_VOLUME)));
                }
            }
            // 遍历采购商封装交易额
            for (Map<String, Object> purchase : purchases) {
                // 根据采购商id查询交易额
                BigDecimal purchaseTradingVolume = purchaseAttributeMap.get(purchase.get(ID));
                if (purchaseTradingVolume == null) {
                    purchase.put(PURCHASE_TRADING_VOLUME, BigDecimal.ZERO);
                } else {
                    purchase.put(PURCHASE_TRADING_VOLUME, purchaseTradingVolume);
                }

            }

        }
    }

    private void appendBidTradingVolume(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryBidSqlTemplate = "SELECT\n" +
                "\tbsp.company_id AS companyId,\n" +
                "\tsum( bs.bid_total_price ) AS bidTradingVolume \n" +
                "FROM\n" +
                "\tbid_sub_project bsp\n" +
                "\tLEFT JOIN bid_supplier bs ON bsp.project_id = bs.project_id \n" +
                "\tAND bsp.id = bs.sub_project_id \n" +
                "\tAND bsp.company_id = bs.company_id \n" +
                "WHERE\n" +
                "\tbsp.project_status IN ( 2, 3 ) \n" +
                "\tAND bs.win_bid_status = 1 \n" +
                "\tAND bs.bid_total_price IS NOT NULL\n" +
                "\tAND bs.company_id in (%s)\n" +
                "GROUP BY\n" +
                "\tbsp.company_id;";

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryBidSql = String.format(queryBidSqlTemplate, purchaserIdToString);
            List<Map<String, Object>> bidTradingVolumeList = DBUtil.query(tenderDataSource, queryBidSql, null);
            HashMap<String, BigDecimal> bidAttributeMap = new HashMap<>();
            if (!CollectionUtils.isEmpty(bidTradingVolumeList)) {
                for (Map<String, Object> bidTradingVolumeMap : bidTradingVolumeList) {
                    bidAttributeMap.put(String.valueOf(bidTradingVolumeMap.get(COMPANY_ID)), ((BigDecimal) bidTradingVolumeMap.get(BID_TRADING_VOLUME)));
                }
            }

            // 计算总的交易额
            for (Map<String, Object> purchaser : purchasers) {
                // 采购商招标额
                BigDecimal bidTradingVolume = bidAttributeMap.get(purchaser.get(ID));
                if (bidTradingVolume != null) {
                    //采购商总交易额
                    purchaser.put(BID_TRADING_VOLUME, bidTradingVolume.toString());
                    BigDecimal tradingVolume = ((BigDecimal) purchaser.get(PURCHASE_TRADING_VOLUME)).add(bidTradingVolume);
                    purchaser.put(TRADING_VOLUME, tradingVolume.toString());
                    purchaser.put(LONG_TRADING_VOLUME, tradingVolume.longValue());
                } else {
                    purchaser.put(BID_TRADING_VOLUME, "0");
                    BigDecimal tradingVolume = (BigDecimal) purchaser.get(PURCHASE_TRADING_VOLUME);
                    purchaser.put(TRADING_VOLUME, tradingVolume.toString());
                    purchaser.put(LONG_TRADING_VOLUME, tradingVolume.longValue());
                }
                // 处理为String类型
                purchaser.put(PURCHASE_TRADING_VOLUME, purchaser.get(PURCHASE_TRADING_VOLUME).toString());
            }
        }
    }

    private void appendPurchaseProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryPurchaserSqlTemplate = "SELECT\n" +
                "\tcount( 1 ) AS purchaseProjectCount,\n" +
                "\tpp.company_id AS companyId \n" +
                "FROM\n" +
                "\tpurchase_project pp \n" +
                "WHERE\n" +
                "\tpp.process_status IN ( 31, 40 )\n" +
                "\tAND pp.company_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tpp.company_id";
        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String querySql = String.format(queryPurchaserSqlTemplate, purchaserIdToString);
            Map<Long, Long> purchaseProjectCountMap = DBUtil.query(purchaseDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
                @Override
                public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                    HashMap<Long, Long> projectCountMap = new HashMap<>();
                    while (resultSet.next()) {
                        projectCountMap.put(resultSet.getLong("companyId"), resultSet.getLong("purchaseProjectCount"));
                    }
                    return projectCountMap;
                }
            });

            for (Map<String, Object> purchaser : purchasers) {
                Long purchaseProjectCount = purchaseProjectCountMap.get(Long.parseLong(((String) purchaser.get(ID))));
                if (purchaseProjectCount == null) {
                    purchaser.put(PURCHASE_PROJECT_COUNT, 0L);
                } else {
                    purchaser.put(PURCHASE_PROJECT_COUNT, purchaseProjectCount);
                }
            }
        }
    }

    private void appendBidProjectCount(List<Map<String, Object>> purchasers, String purchaserIdToString) {
        String queryBidSqlTemplate = "SELECT\n" +
                "\tcount( 1 ) AS bidProjectCount,\n" +
                "\tcompany_id AS companyId \n" +
                "FROM\n" +
                "\tbid_sub_project \n" +
                "WHERE\n" +
                "\tproject_status IN ( 2, 3 ) \n" +
                "\tAND company_id IN (%s) \n" +
                "GROUP BY\n" +
                "\tcompany_id";

        if (!StringUtils.isEmpty(purchaserIdToString)) {
            String queryBidSql = String.format(queryBidSqlTemplate, purchaserIdToString);
            Map<Long, Long> bidProjectCountMap = DBUtil.query(tenderDataSource, queryBidSql, null, new DBUtil.ResultSetCallback<Map<Long, Long>>() {
                @Override
                public Map<Long, Long> execute(ResultSet resultSet) throws SQLException {
                    HashMap<Long, Long> projectCountMap = new HashMap<>();
                    while (resultSet.next()) {
                        projectCountMap.put(resultSet.getLong(COMPANY_ID), resultSet.getLong(BID_PROJECT_COUNT));
                    }
                    return projectCountMap;
                }
            });
            // 计算总的交易额
            for (Map<String, Object> purchaser : purchasers) {
                // 采购商招标项目个数
                Long bidProjectCount = bidProjectCountMap.get(Long.parseLong(((String) purchaser.get(ID))));
                if (bidProjectCount != null) {
                    // 采购商总项目数
                    purchaser.put(BID_PROJECT_COUNT, bidProjectCount);
                    Long projectCount = (Long) purchaser.get(PURCHASE_PROJECT_COUNT) + bidProjectCount;
                    purchaser.put(PROJECT_COUNT, projectCount);
                } else {
                    purchaser.put(BID_PROJECT_COUNT, 0);
                    Long projectCount = (Long) purchaser.get(PURCHASE_PROJECT_COUNT);
                    purchaser.put(PROJECT_COUNT, projectCount);
                }
            }

        }
    }

    private void appendPurchaseRegion(List<Map<String, Object>> purchasers, Set<Long> purchaseIds) {
        Map<Long, AreaUtil.AreaInfo> areaInfoMap = AreaUtil.queryAreaInfo(centerDataSource, purchaseIds);
        for (Map<String, Object> purchaser : purchasers) {
            AreaUtil.AreaInfo areaInfo = areaInfoMap.get(Long.parseLong(((String) purchaser.get(ID))));
            if (areaInfo != null) {
                purchaser.put(REGION, areaInfo.getRegion());
                purchaser.put(AREA_STR, areaInfo.getAreaStr());
                purchaser.put(AREA_STR_NOT_ANALYZED, areaInfo.getAreaStr());
            } else {
                purchaser.put(REGION, null);
                purchaser.put(AREA_STR, null);
                purchaser.put(AREA_STR_NOT_ANALYZED, null);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> purchases) {
//        System.out.println("=============" + purchases);
        if (!CollectionUtils.isEmpty(purchases)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> purchase : purchases) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.purchase"),
                                String.valueOf(purchase.get(ID)))
                        .setSource(JSON.toJSONString(purchase, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    //是date类型按指定日期格式转换
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {

                                    return propertyValue;
                                }
                            }
                        })));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            //是否失败
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

    private void refresh(Map<String, Object> result) {
        result.put(ID, String.valueOf(result.get(ID)));
        // 处理companySiteAlias
        Object companySiteObject = result.get(COMPANY_SITE_ALIAS);
        if (companySiteObject != null) {
            String companySite = String.valueOf(companySiteObject).trim();
            if (!companySite.startsWith("http")) {
                companySite = "http://" + companySite;
            }
            result.put(COMPANY_SITE_ALIAS, companySite);
        }
        result.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    /**
     * 追加分页查询参数
     *
     * @param params
     * @param i
     * @return
     */
    private List<Object> appendToParams(ArrayList<Object> params, long i) {
        List<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
