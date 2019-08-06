package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import com.alibaba.fastjson.JSON;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHander;
import com.xxl.job.core.log.XxlJobLogger;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@JobHander(value = "syncDeleteCompanyJobHandler")
@Service
public class SyncDeleteCompanyJobHandler extends IJobHandler implements InitializingBean {

    private Logger logger= LoggerFactory.getLogger(SyncDeleteCompanyJobHandler.class);

    @Autowired
    protected ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    protected DataSource uniregDataSource;

    @Override
    public ReturnT<String> execute(String... params) throws Exception {
        logger.info("同步删除的企业数据的商机开始");
        deleteCompanyData();
        logger.info("同步删除的企业数据的商机结束");
        return ReturnT.SUCCESS;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        deleteCompanyData();
    }

    private void deleteCompanyData(){
        String countSql="select COUNT(*) from company_index where update_time IS NULL";

        String querySql="select i.id indexId,i.business_id businessId,c.id companyId,i.type FROM company_index i LEFT JOIN t_reg_company c ON i.business_id=c.ID where i.update_time IS NULL LIMIT ?,?";

        String updateSql= "update company_index SET update_time=now() where id in (%s)";

        doDeleteCompanyData(countSql,querySql,updateSql);

    }

    private void doDeleteCompanyData(String countSql,String querySql,String updateSql){
        long count = DBUtil.count(uniregDataSource, countSql, null);
        XxlJobLogger.log("数据库删除条数为{0}",count);
        logger.info("数据库删除条数为{}",count);
        if(count>0){
            int pageSize=1000;
            for (long i = 0; i < count; i = i + pageSize) {
                // 添加分页
                List<Object> paramsToUse = new ArrayList<>();
                paramsToUse.add(i);
                paramsToUse.add(pageSize);
                List<Map<String, Object>> mapList = DBUtil.query(uniregDataSource, querySql, paramsToUse);
                logger.info("执行querySql: {}, params: {}, 共{}条", querySql, paramsToUse, mapList.size());
                List<String> sqlIdList=new ArrayList<>();
                List<String> supplierIdList=new ArrayList<>();
                List<String> purchaseIdList=new ArrayList<>();
                for (Map<String, Object> map:mapList) {
                    String businessId = map.get("businessId").toString();
                    sqlIdList.add(map.get("indexId").toString());
                    if(map.get("companyId")==null){
                        if(map.get("type")!=null && "13".equals(map.get("type").toString())){
                            supplierIdList.add(businessId);
                        }else {
                            purchaseIdList.add(businessId);
                        }
                    }
                }
                //删除es数据
                doDeleteEsData(supplierIdList,purchaseIdList);
                //更新数据库
                String newUpdateSql=String.format(updateSql, StringUtils.collectionToCommaDelimitedString(sqlIdList));
                DBUtil.execute(uniregDataSource, newUpdateSql, null);
            }
        }
    }

    private void doDeleteEsData(List<String> supplierIdList,List<String> purchaseIdList){
        Properties properties = elasticClient.getProperties();
        BulkByScrollResponse deleteSupplierResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                .filter(QueryBuilders.idsQuery().addIds(supplierIdList.toArray(new String[]{})))
                .source(properties.getProperty("cluster.supplier_index")).get();
        if(!CollectionUtils.isEmpty(deleteSupplierResponse.getBulkFailures())){
            logger.error("删除供应商企业数据异常:{}", JSON.toJSONString(deleteSupplierResponse.getBulkFailures()));
        }
        BulkByScrollResponse deletePurchaseResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                .filter(QueryBuilders.idsQuery().addIds(purchaseIdList.toArray(new String[]{})))
                .source(properties.getProperty("cluster.purchase_index")).get();
        if(!CollectionUtils.isEmpty(deletePurchaseResponse.getBulkFailures())){
            logger.error("删除采购商企业数据异常:{}", JSON.toJSONString(deletePurchaseResponse.getBulkFailures()));
        }
        BulkByScrollResponse deleteSupplierProductResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                .filter(QueryBuilders.termsQuery("supplierId", supplierIdList))
                .source(properties.getProperty("cluster.supplier_product_index")).get();
        if(!CollectionUtils.isEmpty(deleteSupplierProductResponse.getBulkFailures())){
            logger.error("删除供应商产品数据异常:{}", JSON.toJSONString(deleteSupplierProductResponse.getBulkFailures()));
        }
        BulkByScrollResponse deleteSupplierProjectResponse = DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getTransportClient())
                .filter(QueryBuilders.termsQuery("supplierId", supplierIdList))
                .source(properties.getProperty("cluster.deal_supplier_project")).get();
        if(!CollectionUtils.isEmpty(deleteSupplierProjectResponse.getBulkFailures())){
            logger.error("删除供应商成交项目数据异常:{}", JSON.toJSONString(deleteSupplierProjectResponse.getBulkFailures()));
        }
    }

    public static void main(String[] args) {
        IdsQueryBuilder queryBuilder = QueryBuilders.idsQuery().addIds("1224", "25544");
        System.out.println(queryBuilder);
    }

}
