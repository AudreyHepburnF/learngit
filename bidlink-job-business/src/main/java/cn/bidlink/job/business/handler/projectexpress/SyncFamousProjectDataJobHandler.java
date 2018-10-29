package cn.bidlink.job.business.handler.projectexpress;

import cn.bidlink.job.business.handler.JobHandler;
import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:同步名企直通车订单数据
 * @Date 2018/10/26
 */
@Service
@JobHander(value = "syncFamousProjectDataJobHandler")
public class SyncFamousProjectDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncFamousProjectDataJobHandler.class);

    @Autowired
    @Qualifier("proDataSource")
    private DataSource proDataSource;

    @Autowired
    @Qualifier("consoleDataSource")
    private DataSource consoleDataSource;

    @Autowired
    private ElasticClient elasticClient;

    private String ID           = "id";
    private String PURCHASER_ID = "purchaserId";
    private String SUPPLIER_ID  = "supplierId";
    private String URL          = "url";


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步名企直通车订单数据");
        syncFamousProjectData();
        logger.info("1.结束同步名企直通车订单数据");
        return ReturnT.SUCCESS;
    }

    private void syncFamousProjectData() {
//        Timestamp lastSyncTime = new Timestamp(0);
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.express_index", "cluster.type.project_express_traffic_statistic",
                QueryBuilders.termQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE));
        logger.info("0.同步名企直通车lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + ",\n" + "syncTime:" + SyncTimeUtil.currentDateToString());
        syncFamousProjectDataService(lastSyncTime);
    }

    private void syncFamousProjectDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tproduct_order \n" +
                "WHERE\n" +
                "\tproduct_category = \"MQZTC000\" \n" +
                "\tAND paid_status_time > ?";

        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tsupplier_id AS supplierId,\n" +
                "\tsupplier_name AS supplierName,\n" +
                "\tpurchaser_name AS purchaserName,\n" +
                "\tpurchaser_id AS purchaserId,\n" +
                "\torder_code AS orderCode,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tpaid_status_time AS paidStatusTime \n" +
                "FROM\n" +
                "\tproduct_order \n" +
                "WHERE\n" +
                "\tproduct_category = \"MQZTC000\" \n" +
                "\tAND paid_status_time > ?" +
                "\tlimit ?,?";
        doSyncFamousProjectDataService(countSql, querySql, Collections.singletonList(lastSyncTime));

    }

    private void doSyncFamousProjectDataService(String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(proDataSource, countSql, params);
        logger.info("执行countSql:{}, 参数:{}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (Integer i = 0; i < count; i += pageSize) {
                List<Object> appendToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(proDataSource, querySql, appendToUse);
                logger.info("执行querySql:{}, 参数:{}, 共{}条", querySql, appendToUse, mapList);
                List<Long> purchaserIds = new ArrayList<>();
                for (Map<String, Object> map : mapList) {
                    purchaserIds.add(Long.valueOf(map.get(PURCHASER_ID).toString()));
                    refresh(map);

                }
                // http://testbl2017.app.bidlink.cn/portal_tenant/portal/app/companyInfo.htm?companyId=1112798881&orderId=1933
                // 添加采购商默认域名
                appendPurchaserDefaultDomain(purchaserIds, mapList);

                batchInsert(mapList);
            }
        }
    }

    /**
     * 查询采购商默认域名
     *
     * @param purchaserIds
     */
    private void appendPurchaserDefaultDomain(List<Long> purchaserIds, List<Map<String, Object>> mapList) {
        String queryDomainTemplate = "SELECT\n" +
                "\tDEFAULT_DOMAIN,\n" +
                "\tCENTRAL_ID \n" +
                "FROM\n" +
                "\t`pt_tenant` \n" +
                "WHERE\n" +
                "\tCENTRAL_ID IN (%s)";
        String querySql = String.format(queryDomainTemplate, StringUtils.collectionToCommaDelimitedString(purchaserIds));
        logger.info("3.查询采购商默认域名执行querySql:{}", querySql);
        Map<Long, Object> purchaserDomainMap = DBUtil.query(consoleDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Object>>() {


            @Override
            public Map<Long, Object> execute(ResultSet resultSet) throws SQLException {
                Map<Long, Object> map = new HashMap<>();
                while (resultSet.next()) {
                    String defaultDomain = resultSet.getString(1);
                    long purchaserId = resultSet.getLong(2);
                    map.put(purchaserId, defaultDomain);
                }
                return map;
            }
        });
        // http://testbl2017.app.bidlink.cn/portal_tenant/portal/app/companyInfo.htm?companyId=1112798881&orderId=1933
        if (!CollectionUtils.isEmpty(purchaserDomainMap)) {
            mapList.forEach(map -> {
                Object defaultDomain = purchaserDomainMap.get(Long.valueOf(map.get(PURCHASER_ID).toString()));
                if (!Objects.isNull(defaultDomain)) {
                    map.put(URL, defaultDomain
                            + "/portal_tenant/portal/app/companyInfo.htm?companyId="
                            + "companyId=" + map.get(SUPPLIER_ID) + "&orderId=" + map.get(ID));
                }
            });
        }
    }

    private void refresh(Map<String, Object> map) {
        // 防止精度丢失
        map.put(ID, String.valueOf(map.get(ID)));
        map.put(PURCHASER_ID, String.valueOf(map.get(PURCHASER_ID)));
        map.put(SUPPLIER_ID, String.valueOf(map.get(SUPPLIER_ID)));

        // 来源
        map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
        map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            Properties properties = elasticClient.getProperties();
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient().prepareUpdate(properties.getProperty("cluster.express_index"),
                        properties.getProperty("cluster.type.project_express_traffic_statistic"), String.valueOf(map.get(ID)))
                        .setDoc(JSON.toJSONString(map, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyKey, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    return SyncTimeUtil.toDateString(propertyValue);
                                }
                                return propertyValue;
                            }
                        }))
                        // 更新数据时,没有则插入
                        .setDocAsUpsert(true)
                )
                ;
            }
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                logger.error("插入名企直通车数据报错,异常信息:{}", bulkResponse.buildFailureMessage());
            }
        }

    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
