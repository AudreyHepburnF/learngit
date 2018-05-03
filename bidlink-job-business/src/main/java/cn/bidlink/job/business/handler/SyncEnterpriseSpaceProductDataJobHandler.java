package cn.bidlink.job.business.handler;

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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">zhouzhihui</a>
 * @version Ver 1.0
 * @description:
 * @Date 2018/2/27
 */
@JobHander(value = "syncEnterpriseSpaceDataJobHandler")
@Service
public class SyncEnterpriseSpaceProductDataJobHandler extends IJobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncEnterpriseSpaceProductDataJobHandler.class);

    @Autowired
    @Qualifier("enterpriseSpaceDataSource")
    private DataSource enterpriseSpaceDataSource;

//    @Autowired
//    @Qualifier("centerDataSource")
//    private DataSource centerDataSource;

    @Autowired
    private ElasticClient elasticClient;

    @Value("${pageSize}")
    private Integer pageSize;

    private String ID                        = "id";
    private String CORE                      = "core";
    private String COMPANY_ID                = "companyId";
    private String PRODUCT_NAME              = "productName";
    private String PRODUCT_NAME_NOT_ANALYZED = "productNameNotAnalyzed";
    private String ZONE_STR                  = "zoneStr";
    private String ZONE_STR_NOT_ANALYZED     = "zoneStrNotAnalyzed";


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("同步企业空间产品开始");
        syncEnterpriseSpaceData();
        logger.info("同步企业空间产品结束");
        return ReturnT.SUCCESS;
    }

    private void syncEnterpriseSpaceData() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient,
                "cluster.index",
                "cluster.type.enterprise_space_product",
                null);
        logger.info("企业空间同步lastTime:" + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss") + "\n" +
                ", syncTime:" + new DateTime(SyncTimeUtil.getCurrentDate()).toString("yyyy-MM-dd HH:mm:ss"));
        syncCreateEnterpriseSpaceService(lastSyncTime);
        syncUpdateEnterpriseSpaceService(lastSyncTime);
    }

    /**
     * 同步更新企业空间产品
     *
     * @param lastTime
     */
    private void syncUpdateEnterpriseSpaceService(Timestamp lastTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tspace_product" +
                "\tWHERE update_time > ? \n";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tCOMPANYID AS companyId,\n" +
                "\tPICFILE AS picFile,\n" +
                "\ttitle AS productName,\n" +
                "\tCOMPANYNAME AS companyName,\n" +
                "\tSTATE AS state,\n" +
                "\tZONESTR AS zoneStrNotAnalyzed,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tNOTICESTATE AS noticeState,\n" +
                "\tZONESTR AS zoneStr\n" +
                "FROM\n" +
                "\tspace_product \n" +
                "\tWHERE update_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastTime);
        doSyncEnterpriseSpaceService(countSql, querySql, params);
    }

    /**
     * 同步插入企业空间产品
     *
     * @param lastTime
     */
    private void syncCreateEnterpriseSpaceService(Date lastTime) {
        String countSql = "SELECT\n" +
                "\tcount(1)\n" +
                "FROM\n" +
                "\tspace_product" +
                "\tWHERE create_time > ? \n";
        String querySql = "SELECT\n" +
                "\tid,\n" +
                "\tCOMPANYID AS companyId,\n" +
                "\tPICFILE AS picFile,\n" +
                "\ttitle AS productName,\n" +
                "\tCOMPANYNAME AS companyName,\n" +
                "\tSTATE AS state,\n" +
                "\tZONESTR AS zoneStrNotAnalyzed,\n" +
                "\tcreate_time AS createTime,\n" +
                "\tNOTICESTATE AS noticeState,\n" +
                "\tZONESTR AS zoneStr\n" +
                "FROM\n" +
                "\tspace_product \n" +
                "\tWHERE create_time > ? \n" +
                "\tLIMIT ?,?";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastTime);
        doSyncEnterpriseSpaceService(countSql, querySql, params);
    }

    private void doSyncEnterpriseSpaceService(String countSql, String querySql, ArrayList<Object> params) {
        long count = DBUtil.count(enterpriseSpaceDataSource, countSql, params);
        logger.debug("执行countSql:{}, params:{} , 共{}条", countSql, params, count);
        for (long i = 0; i < count; i += pageSize) {
            List<Object> paramsToUse = appendToParams(params, i);
            List<Map<String, Object>> products = DBUtil.query(enterpriseSpaceDataSource, querySql, paramsToUse);

            // 添加供应商状态 FIXME 待设计核心供 默认为核心供
//            appendSupplierStatus(products);
            for (Map<String, Object> product : products) {
                product.put(CORE, 1);
            }

            // 添加同步时间和字段类型转换
            for (Map<String, Object> product : products) {
                refresh(product);
            }
            // 插入es中
            batchInsert(products);
        }
    }

    /**
     * 添加供应商状态 core: 0表示非核心供    1:核心供
     *
     * @param mapList
     */
//    private void appendSupplierStatus(List<Map<String, Object>> mapList) {
//        HashSet<Long> companyIds = new HashSet<>();
//        for (Map<String, Object> map : mapList) {
//            companyIds.add(((Long) map.get(COMPANY_ID)));
//        }
//
//        String querySqlTemplate = "SELECT\n" +
//                "\tCOMP_ID AS companyId,\n" +
//                "\tCREDIT_MEDAL_STATUS AS core \n" +
//                "FROM\n" +
//                "\t`t_uic_company_status` \n" +
//                "WHERE\n" +
//                "\tCOMP_ID IN (%s)";
//        String querySql = String.format(querySqlTemplate, StringUtils.collectionToCommaDelimitedString(companyIds));
//        Map<Long, Integer> coreMap = DBUtil.query(centerDataSource, querySql, null, new DBUtil.ResultSetCallback<Map<Long, Integer>>() {
//            @Override
//            public Map<Long, Integer> execute(ResultSet resultSet) throws SQLException {
//                HashMap<Long, Integer> map = new HashMap<>();
//                while (resultSet.next()) {
//                    map.put(resultSet.getLong(COMPANY_ID), resultSet.getInt(CORE));
//                }
//                return map;
//            }
//        });
//
//        for (Map<String, Object> map : mapList) {
//            map.put(CORE, coreMap.get(map.get(COMPANY_ID)));
//        }
//    }

    /**
     * 添加分页查询
     *
     * @param params
     * @param i
     * @return
     */
    private List<Object> appendToParams(ArrayList<Object> params, long i) {
        ArrayList<Object> paramsToUse = new ArrayList<>(params);
        paramsToUse.add(i);
        paramsToUse.add(pageSize);
        return paramsToUse;
    }

    /**
     * 添加同步时间字段
     *
     * @param product
     */
    private void refresh(Map<String, Object> product) {
        product.put(ID, String.valueOf(product.get(ID)));
        product.put(COMPANY_ID, String.valueOf(product.get(COMPANY_ID)));
        product.put(ZONE_STR_NOT_ANALYZED, product.get(ZONE_STR));
        product.put(PRODUCT_NAME_NOT_ANALYZED, product.get(PRODUCT_NAME));
        product.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
    }

    /**
     * 批量插入es
     *
     * @param products
     */
    private void batchInsert(List<Map<String, Object>> products) {
        System.out.println(products);
        if (!CollectionUtils.isEmpty(products)) {
            BulkRequestBuilder bulkRequest = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> product : products) {
                bulkRequest.add(elasticClient.getTransportClient()
                        .prepareIndex(elasticClient.getProperties().getProperty("cluster.index"),
                                elasticClient.getProperties().getProperty("cluster.type.enterprise_space_product"),
                                String.valueOf(product.get(ID)))
                        .setSource(JSON.toJSONString(product, new ValueFilter() {
                            @Override
                            public Object process(Object object, String propertyName, Object propertyValue) {
                                if (propertyValue instanceof java.util.Date) {
                                    // 是date类型按指定日期格式转换
                                    return new DateTime(propertyValue).toString(SyncTimeUtil.DATE_TIME_PATTERN);
                                } else {

                                    return propertyValue;
                                }
                            }
                        })));
            }
            BulkResponse response = bulkRequest.execute().actionGet();
            // 是否失败
            if (response.hasFailures()) {
                logger.error(response.buildFailureMessage());
            }
        }
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
