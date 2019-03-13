package cn.bidlink.job.ycsearch.handler.projectexpress;

import cn.bidlink.job.common.constant.BusinessConstant;
import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import cn.bidlink.job.ycsearch.handler.JobHandler;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:悦采供应商交易数据统计,给项目直通车查询
 * @Date 2018/11/2
 */
@Service
@JobHander(value = "syncYcSupplierProjectDataJobHandler")
public class SyncYcSupplierProjectDataJobHandler extends JobHandler /*implements InitializingBean*/ {

    private Logger logger = LoggerFactory.getLogger(SyncYcSupplierProjectDataJobHandler.class);

    @Autowired
    private ElasticClient elasticClient;

    private   String ID                         = "id";
    private   String COMPANY_NAME               = "companyName";
    protected String AREA_STR                   = "areaStr";
    protected String MAIN_PRODUCT               = "mainProduct";
    private   String TOTAL_COOPERATED_PURCHASER = "totalCooperatedPurchaser";
    private   String TOTAL_DEAL_PRICE           = "totalDealPrice";

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("0.开始同步悦采供应商成交量和合作供应商统计");
        syncYcSupplierProjectData();
        logger.info("1.结束同步悦采供应商成交量和合作供应商统计");
        return ReturnT.SUCCESS;
    }

    private void syncYcSupplierProjectData() {
        Properties properties = elasticClient.getProperties();
        int pageUseSize = 1000;
        SearchResponse response = elasticClient.getTransportClient().prepareSearch(properties.getProperty("cluster.supplier_index"))
                .setTypes(properties.getProperty("cluster.type.supplier"))
                .setQuery(QueryBuilders.termQuery(BusinessConstant.WEB_TYPE, BusinessConstant.YUECAI))
                .setScroll(new TimeValue(60000))
                .setSize(pageUseSize)
                .execute().actionGet();
        do {
            SearchHits hits = response.getHits();
            if (hits.getTotalHits() > 0) {
                List<Map<String, Object>> resultFromEs = new ArrayList<>();
                for (SearchHit hit : hits.getHits()) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    resultFromEs.add(source);
                }
                // 处理字段
                List<Map<String, Object>> mapList = this.handlerColumn(resultFromEs);
                batchInsert(mapList);
                response = elasticClient.getTransportClient().prepareSearchScroll(response.getScrollId())
                        .setScroll(new TimeValue(60000))
                        .execute().actionGet();
            }
        } while (response.getHits().getHits().length != 0);
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            Properties properties = elasticClient.getProperties();
            BulkRequestBuilder requestBuilder = elasticClient.getTransportClient().prepareBulk();
            for (Map<String, Object> map : mapList) {
                requestBuilder.add(elasticClient.getTransportClient().prepareIndex(properties.getProperty("cluster.supplier_express_index"),
                        properties.getProperty("cluster.type.supplier_express"))
                        .setId(String.valueOf(map.get(ID)))
                        .setSource(SyncTimeUtil.handlerDate(map))
                );
            }
            BulkResponse response = requestBuilder.execute().actionGet();
            if (response.hasFailures()) {
                logger.error("悦采供应商数据插入失败,异常信息:{}", response.buildFailureMessage());
            }
        }
    }

    private List<Map<String, Object>> handlerColumn(List<Map<String, Object>> resultFromEs) {
        return resultFromEs.stream().map(map -> {
            Map<String, Object> result = new HashMap<>(6);
            result.put(ID, map.get(ID));
            result.put(COMPANY_NAME, map.get(COMPANY_NAME));
            result.put(TOTAL_COOPERATED_PURCHASER, map.get(TOTAL_COOPERATED_PURCHASER));
            result.put(AREA_STR, map.get(AREA_STR));
            result.put(MAIN_PRODUCT, map.get(MAIN_PRODUCT));
            result.put(TOTAL_DEAL_PRICE, map.get(TOTAL_DEAL_PRICE));
            return result;
        }).collect(Collectors.toList());
    }

    private Map<String, Long> getSupplierStatMap(DataSource dataSource, String querySqlTemplate, String supplierIds) {
        String querySql = String.format(querySqlTemplate, supplierIds);
        return DBUtil.query(dataSource, querySql, null, new DBUtil.ResultSetCallback<Map<String, Long>>() {
            @Override
            public Map<String, Long> execute(ResultSet resultSet) throws SQLException {
                Map<String, Long> map = new HashMap<>();
                while (resultSet.next()) {
                    map.put(String.valueOf(resultSet.getLong(2)), resultSet.getLong(1));
                }
                return map;
            }
        });
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
