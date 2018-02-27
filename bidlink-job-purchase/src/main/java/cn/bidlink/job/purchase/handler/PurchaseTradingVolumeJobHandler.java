package cn.bidlink.job.purchase.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:同步采购商和交易额
 * @Date 2017/11/29
 */
@Service
@JobHander(value = "purchaseTradingVolumeJobHandler")
public class PurchaseTradingVolumeJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(PurchaseTradingVolumeJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("ycDataSource")
    private DataSource ycDataSource;

    @Autowired
    @Qualifier("centerDataSource")
    private DataSource centerDataSource;

    @Value("${pageSize}")
    private int pageSize;

    private String ID = "id";
    private String PURCHASE_TRADING_VOLUME = "purchaseTradingVolume";
    private String BID_TRADING_VOLUME = "bidTradingVolume";
    private String TRADING_VOLUME = "tradingVolume";
    private String LONG_TRADING_VOLUME = "longTradingVolume";
    private String COMPANY_SITE_ALIAS = "companySiteAlias";




    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        //时间和线程绑定
        SyncTimeUtil.setCurrentDate();
        logger.info("同步采购商开始");
        synPurchase();
        logger.info("同步采购商结束");
        return ReturnT.SUCCESS;
    }

    /**
     * 同步采购商
     */
    private void synPurchase() {
        //查询es中同步采购商最后时间
        Timestamp lastSyncTime = new Timestamp(0);
        logger.debug("同步采购商lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));
        //同步插入数据
        syncCreatePurchaseProjectData(lastSyncTime);
        //同步更新数据
        syncUpdatedPurchaseProjectData(lastSyncTime);
    }

    /**
     * 同步更新采购商
     *
     * @param lastSyncTime
     */
    private void syncUpdatedPurchaseProjectData(Timestamp lastSyncTime) {
        String countUpdatedPurchaseSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND  trc.update_time >= ?";

        String queryUpdatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND   trc.update_time >= ?\n"
                + "LIMIT ?, ?";

        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncProjectDataService(countUpdatedPurchaseSql, queryUpdatedPurchaseSql, params);
    }

    /**
     * 同步插入采购商
     *
     * @param lastSyncTime
     */
    private void syncCreatePurchaseProjectData(Timestamp lastSyncTime) {
        String countCreatedPurchaseSql = "SELECT\n"
                + "   count(1)\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND  trc.create_date >= ?";

        String queryCreatedPurchaseSql = "SELECT\n"
                + "   trc.id,\n"
                + "   trc.name AS purchaseName,\n"
                + "   trc.WWW_STATION AS wwwStationAlias,\n"
                + "   trc.company_site AS companySiteAlias\n"
                + "FROM\n"
                + "   t_reg_company trc\n"
                + "WHERE\n"
                + "     trc.TYPE = 12\n"
                + "AND  trc.create_date >= ?\n"
                + "LIMIT ?, ?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        //同步数据到es中
        doSyncProjectDataService(countCreatedPurchaseSql, queryCreatedPurchaseSql, params);
    }

    /**
     * 同步数据到es中
     *
     * @param countSql 查询总条数sql
     * @param querySql 查询结果集sql
     * @param params   参数 lastSyncTime es中最后同步时间
     */
    private void doSyncProjectDataService(String countSql, String querySql, ArrayList<Object> params) {
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
                ArrayList<Long> purchaseIds = new ArrayList<>();
                for (Map<String, Object> purchaser : purchasers) {
                    purchaseIds.add((Long) purchaser.get(ID));
                    // 添加同步时间
                    refresh(purchaser);
                }

                // 添加采购商交易额
                appendPurchaseTradingVolume(purchasers, purchaseIds);
                // 添加招标交易额
                appendBidTradingVolume(purchasers, purchaseIds);
                // 批量插入es中
                batchInsert(purchasers);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> purchases) {
//        for (Map<String, Object> purchase : purchases) {
//            System.out.println(JSON.toJSONString(purchase, new ValueFilter() {
//                @Override
//                public Object process(Object object, String propertyName, Object propertyValue) {
//                    if (propertyValue instanceof java.util.Date) {
//                        //是date类型按指定日期格式转换
//                        return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
//                    } else {
//
//                        return propertyValue;
//                    }
//                }
//            }));
//        }


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
                                if (propertyValue instanceof java.util.Date) {
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


    /**
     * 批量查询采购商的招标额
     *
     * @param purchasers
     * @param purchaseIds
     */
    private void appendBidTradingVolume(List<Map<String, Object>> purchasers, ArrayList<Long> purchaseIds) {
        String queryBidSqlTemplate = "SELECT\n"
                + "   bp.company_id AS id,\n"
                + "   sum(bp.total_bid_price) AS bidTradingVolume\n"
                + "FROM\n"
                + "   bid_product bp\n"
                + "WHERE bp.total_bid_price is not null\n"
                + "AND bp.company_id IN (%s)\n"
                + "GROUP BY\n"
                + "     bp.company_id\n";

        if (!CollectionUtils.isEmpty(purchaseIds)) {
            String queryBidSql = String.format(queryBidSqlTemplate, StringUtils.collectionToCommaDelimitedString(purchaseIds));
            List<Map<String, Object>> bidTradingVolumeList = DBUtil.query(ycDataSource, queryBidSql, null);
            HashMap<String, BigDecimal> bidAttributeMap = new HashMap<>();
            if (!CollectionUtils.isEmpty(bidTradingVolumeList)) {
                for (Map<String, Object> bidTradingVolumeMap : bidTradingVolumeList) {
                    bidAttributeMap.put(String.valueOf(bidTradingVolumeMap.get(ID)), ((BigDecimal)bidTradingVolumeMap.get(BID_TRADING_VOLUME)));
                }
            }

            //计算总的交易额
            for (Map<String, Object> purchaser : purchasers) {
                //采购商招标额
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
                //处理为String类型
                purchaser.put(PURCHASE_TRADING_VOLUME, purchaser.get(PURCHASE_TRADING_VOLUME).toString());
            }


        }
    }

    /**
     * 批量查询采购商的采购额
     *
     * @param purchases
     * @param purchaseIds
     */
    private void appendPurchaseTradingVolume(List<Map<String, Object>> purchases, ArrayList<Long> purchaseIds) {
        String queryPurchaserSqlTemplate = "SELECT\n"
                + "   sum(bpe.deal_total_price) AS purchaseTradingVolume ,\n"
                + "   bp.comp_id AS id\n"
                + "FROM\n"
                + "   bmpfjz_project bp\n"
                + "LEFT JOIN bmpfjz_project_ext bpe ON bp.id = bpe.id AND bp.comp_id = bpe.comp_id\n"
                + "AND bp.project_status IN (8, 9)\n"
                + "WHERE bpe.deal_total_price is not null\n"
                + "AND bp.comp_id IN (%s)\n"
                + "GROUP BY\n"
                + "   bp.comp_id";

        if (!CollectionUtils.isEmpty(purchaseIds)) {
            String queryPurchaseSql = String.format(queryPurchaserSqlTemplate, StringUtils.collectionToCommaDelimitedString(purchaseIds));
            //根据采购商id查询交易额
            List<Map<String, Object>> purchaseTradingVolumeList = DBUtil.query(ycDataSource, queryPurchaseSql, null);
            HashMap<String, BigDecimal> purchaseAttributeMap = new HashMap<>();
            //list<Map>转换为map
            if (!CollectionUtils.isEmpty(purchaseTradingVolumeList)) {
                for (Map<String, Object> map : purchaseTradingVolumeList) {
                    purchaseAttributeMap.put(String.valueOf(map.get(ID)), ((BigDecimal)map.get(PURCHASE_TRADING_VOLUME)));
                }
            }
            //遍历采购商封装交易额
            for (Map<String, Object> purchase : purchases) {
                //根据采购商id查询交易额
                BigDecimal purchaseTradingVolume = purchaseAttributeMap.get(purchase.get(ID));
                if (purchaseTradingVolume == null) {
                    purchase.put(PURCHASE_TRADING_VOLUME, BigDecimal.ZERO);
                } else {
                    purchase.put(PURCHASE_TRADING_VOLUME, purchaseTradingVolume);
                }

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
