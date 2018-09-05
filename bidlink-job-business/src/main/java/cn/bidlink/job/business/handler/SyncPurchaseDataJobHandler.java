package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.AreaUtil;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.joda.time.DateTime;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0 采购商企业基本数据同步 {@link SyncPurchaseProjectDataJobHandler  采购商项目数据}
 * @Date 2017/11/29
 */
@Service
@JobHander(value = "syncPurchaseDataJobHandler")
public class SyncPurchaseDataJobHandler extends AbstractSyncPurchaseDataJobHandler/* implements InitializingBean */{

    private Integer SYNC_WAY_CREATE = 1;
    private Integer SYNC_WAY_UPDATE = 2;

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
//        syncOldPurchaserDataService(lastSyncTime);
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
                + "AND trc.create_time < \"2017-06-27 14:51:13\"";
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
                "\tAND trc.create_time < \"2017-06-27 14:51:13\"" +
                "\tAND  trc.update_time >= ?\n" +
                "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaserDataService(countUpdatedPurchaseSql, queryUpdatedPurchaseSql, params, SYNC_WAY_CREATE);
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
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND trc.EXAMINE_STATUS = 2\n"
                + "AND  trc.create_time >= ?";
        String queryCreatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.INDUSTRY_STR AS industryStr,\n"
                + "   trc.INDUSTRY AS industry,\n"
                + "   trc.ZONE_STR AS zoneStr,\n"
                + "   trc.COMP_TYPE_STR AS compTypeStr,\n"
                + "    trc.company_logo AS companyLogo,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND trc.EXAMINE_STATUS = 2\n"
                + "AND  trc.create_time >= ?\n"
                + "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaserDataService(countCreatedPurchaseSql, queryCreatedPurchaseSql, params, SYNC_WAY_CREATE);
    }

    private void syncUpdatedPurchaserData(Timestamp lastSyncTime) {
        String countUpdatedPurchaseSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND trc.EXAMINE_STATUS = 2\n"
                + "AND  trc.update_time >= ?";
        String queryUpdatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.INDUSTRY_STR AS industryStr,\n"
                + "   trc.INDUSTRY AS industry,\n"
                + "   trc.ZONE_STR AS zoneStr,\n"
                + "   trc.COMP_TYPE_STR AS compTypeStr,\n"
                + "    trc.company_logo AS companyLogo,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND trc.EXAMINE_STATUS = 2\n"
                + "AND   trc.update_time >= ?\n"
                + "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncPurchaserDataService(countUpdatedPurchaseSql, queryUpdatedPurchaseSql, params, SYNC_WAY_UPDATE);
    }

    /**
     * 同步数据到es中
     *
     * @param countSql 查询总条数sql
     * @param querySql 查询结果集sql
     * @param params   参数 lastSyncTime es中最后同步时间
     * @param syncWay  同步方式 1:插入 2:更新
     */
    private void doSyncPurchaserDataService(String countSql, String querySql, ArrayList<Object> params, Integer syncWay) {
        long count = DBUtil.count(uniregDataSource, countSql, params);
        logger.debug("执行countSql: {}, params: {}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询的参数
                List<Object> paramsToUse = appendToParams(params, i);
                // 查询分页结果集
                List<Map<String, Object>> purchasers = DBUtil.query(uniregDataSource, querySql, paramsToUse);
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

                // 添加采购商交易量信息,从es中查询
                appendPurchaseTradingInfo(purchasers, purchaserIds, syncWay);
            }
        }
    }

    /**
     * 当更新企业数据时
     *
     * @param purchasers
     * @param purchaserIds
     * @param syncWay      2:更新企业数据
     */
    private void appendPurchaseTradingInfo(List<Map<String, Object>> purchasers, HashSet<Long> purchaserIds, Integer syncWay) {
        if (SYNC_WAY_UPDATE.equals(syncWay)) {
            SearchResponse response = elasticClient.getTransportClient().prepareSearch(elasticClient.getProperties().getProperty("cluster.index"))
                    .setTypes(elasticClient.getProperties().getProperty("cluster.type.purchase"))
                    .setQuery(QueryBuilders.termsQuery("id", purchaserIds))
                    .setSize(purchaserIds.size())
                    .execute().actionGet();
            List<Map<String, Object>> resultFromEs = new ArrayList<>();
            for (SearchHit hit : response.getHits().getHits()) {
                resultFromEs.add(hit.getSource());
            }

            // 拷贝最新的企业数据
            List<Map<String, Object>> mapList = resultFromEs.stream()
                    .map(esMap -> purchasers.stream()
                            .filter(m -> Objects.equals(m.get("id"), esMap.get("id")))
                            .findFirst().map(m -> {
                                esMap.putAll(m);
                                return esMap;
                            }).orElse(null))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            batchInsert(mapList);

        } else {
            batchInsert(purchasers);
        }

    }


    private void appendPurchaseRegion(List<Map<String, Object>> purchasers, Set<Long> purchaseIds) {
        Map<Long, AreaUtil.AreaInfo> areaInfoMap = AreaUtil.queryAreaInfo(uniregDataSource, purchaseIds);
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

    private void refresh(Map<String, Object> result) {
        result.put(ID, String.valueOf(result.get(ID)));
        result.put(INDUSTRY_STR_NOT_ANALYZED, result.get(INDUSTRY_STR));
        result.put(ZONE_STR_NOT_ANALYZED, result.get(ZONE_STR));
        result.put(PURCHASE_NAME_NOT_ANALYZED, result.get(PURCHASE_NAME));
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

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
