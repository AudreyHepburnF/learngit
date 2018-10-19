package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.ValueFilter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.*;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/10/16
 */
@Service
@JobHander(value = "syncProjectExpressDataJobHandler")
public class SyncProjectExpressDataJobHandler extends JobHandler implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(SyncProjectExpressDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier(value = "proDataSource")
    private DataSource proDataSource;

    private String ID                = "id";
    private String SUPPLIER_ID       = "supplierId";
    private String CREATE_USER_ID    = "createUserId";
    private String PRODUCT_CODE      = "productCode";
    private String MATCH_TIMES       = "matchTimes";
    private String ES_ORDER_STATUS   = "esOrderStatus";
    private String END_DATE          = "endDate";
    private String LATEST_MATCH_TIME = "latestMatchTime";


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步悦采项目直通车数据");
        syncProjectExpressData();
        logger.info("1.结束同步悦采项目直通车数据");
        return ReturnT.SUCCESS;
    }

    private void syncProjectExpressData() {
//        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.express_index", "cluster.type.project_express",
//                QueryBuilders.boolQuery().must(QueryBuilders.termsQuery(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE))
//        );
        Timestamp lastSyncTime = new Timestamp(0);
        logger.info("同步悦采项目直通车数据lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + "\n, syncTime:" + SyncTimeUtil.currentDateToString());
        syncProjectExpressDataService(lastSyncTime);
    }

    private void syncProjectExpressDataService(Timestamp lastSyncTime) {
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tproduct_order po\n" +
                "\tINNER JOIN project_express pe ON po.id = pe.product_order_id \n" +
                "WHERE\n" +
                "\tpo.product_category = 'XMZTC000'\n" +
                "\tand po.paid_status_time > ?";

        String querySql = "SELECT\n" +
                "\tpo.id,\n" +
//                "\tpo.purchaser_id AS purchaserId,\n" +
//                "\tpo.purchaser_name AS purchaserName,\n" +
                "\tpo.user_name AS userName,\n" +
                "\tpo.supplier_id AS supplierId,\n" +
                "\tpo.supplier_name AS supplierName,\n" +
                "\tpo.product_code AS productCode,\n" +
                "\tpo.product_name AS productName,\n" +
                "\tpo.order_status AS orderStatus,\n" +
                "\tpo.order_status_time AS orderStatusTime,\n" +
                "\tpo.paid_status AS paidStatus,\n" +
                "\tpo.paid_status_time AS paidStatusTime,\n" +
                "\tpo.start_date AS startDate,\n" +
                "\tpo.end_date AS endDate,\n" +
                "\tpo.create_time AS createTime,\n" +
                "\tpo.creator_id AS createUserId,\n" +
                "\tpo.order_code AS orderCode,\n" +
                "\tpe.keywords AS keywords,\n" +
                "\tpe.match_times AS matchTimes,\n" +
                "\tpe.last_match_date AS latestMatchTime \n" +
                "FROM\n" +
                "\tproduct_order po\n" +
                "\tINNER JOIN project_express pe ON po.id = pe.product_order_id \n" +
                "WHERE\n" +
                "\tpo.product_category = 'XMZTC000'\n" +
                "\tand po.paid_status_time > ? limit ?,?\n" +
                "\t";
        doSyncProjectExpressDataService(proDataSource, countSql, querySql, Collections.singletonList(lastSyncTime));
    }

    private void doSyncProjectExpressDataService(DataSource proDataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(proDataSource, countSql, params);
        logger.info("执行countSql:{},总条数:{}", countSql, count);
        if (count > 0) {
            for (int i = 0; i <= count; i += pageSize) {
                List<Object> appendToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(proDataSource, querySql, appendToUse);
                logger.info("执行querySql:{}, 参数params:{}, 总条数:{}", querySql, appendToUse, mapList.size());
                for (Map<String, Object> map : mapList) {
                    refresh(map);
                }
                batchInsert(mapList);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.express_index")
                                , elasticClient.getProperties().getProperty("cluster.type.project_express"))
                        .setId(String.valueOf(map.get(ID))).setSource(JSON.toJSONString(map, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof Date) {
                                    return SyncTimeUtil.toDateString(propertyValue);
                                }
                                return propertyValue;
                            }
                        })));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            if (response.hasFailures()) {
                logger.info("插入项目直通车数据失败,错误信息:{}", response.buildFailureMessage());
            }
        }
    }

    private void refresh(Map<String, Object> map) {
        // Long转String 类型
        map.put(ID, String.valueOf(map.get(ID)));
        map.put(SUPPLIER_ID, String.valueOf(map.get(SUPPLIER_ID)));
        map.put(CREATE_USER_ID, String.valueOf(map.get(CREATE_USER_ID)));
        Object matchTimes = map.get(MATCH_TIMES);
        if (!Objects.isNull(matchTimes) && matchTimes.equals(map.get(PRODUCT_CODE))) {
            // 匹配次数用完 0:代表用完 1:代表未用完
            map.put(ES_ORDER_STATUS, 0);
            if (Objects.isNull(map.get(END_DATE))) {
                map.put(END_DATE, map.get(LATEST_MATCH_TIME));
            }
        } else {
            map.put(ES_ORDER_STATUS, 1);
        }
        map.put(BusinessConstant.PLATFORM_SOURCE_KEY, BusinessConstant.YUECAI_SOURCE);
        map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }
}
