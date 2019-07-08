package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.es.ElasticClient;
import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.ElasticClientUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.apache.http.util.TextUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">ruifenghan</a>
 * @version Ver 1.0
 * @description:同步采购品
 * @Date 2018/07/03
 */
@Service
@JobHander("syncMaterialsDataJobHandler")
public class SyncMaterialsDataJobHandler extends JobHandler {//implements InitializingBean

    @Autowired
    private ElasticClient elasticClient;

    @Autowired
    @Qualifier("uniregDataSource")
    private DataSource uniregDataSource;

    private   Logger logger                = LoggerFactory.getLogger(SyncMaterialsDataJobHandler.class);

    private String    ID                         = "id";
    private String    COMPANY_ID                 = "companyId";
    private String    CATALOGID                  = "catalogId";
    private String    CATALOGNAMEPATh            = "catalogNamePath";
    private String    CATALOGIDPATH              = "catalogIdPath";
    private String    CODE                       = "code";
    private String    NAME                       = "name";
    private String    NAMENOTANALYZED            = "nameNotAnalyzed";
    private String    SPEC                       = "spec";
    private String    ABANDON                    = "abandon";
    private String    PCODE                      = "pcode";
    private String    PRODUCTOR                  = "productor";
    private String    UNITNAME                   = "unitName";
    private String    PRODUCINGADDRESS           = "producingAddress";
    private String    BRAND                      = "brand";
    private String    PURPOSE                    = "purpose";
    private String    MARKETPRICE                = "marketPrice";
    private String    SPECIALITY                 = "speciality";
    private String    SOURCE                     = "source";
    private String    TECHPARAMETERS             = "techParameters";
    private String    DEMO                       = "demo";
    private String    UNITPRECISION              = "unitPrecision";
    private String    PRICEPRECISION             = "pricePrecision";
    private String    IMGCODE                    = "imgCode";
    private String    ORGIDARR                   = "orgIdArr";
    private String    SYNCTIME                   = "syncTime";
    private String    UPDATETIME                 = "updateTime";
    private String    CREATETIME                 = "createTime";


    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        SyncTimeUtil.setCurrentDate();
        logger.info("开始同步采购品");
        syncMaterials();
        logger.info("结束同步采购品");
        return ReturnT.SUCCESS;
    }

    private void syncMaterials() {
        Timestamp lastSyncTime = ElasticClientUtil.getMaxTimestamp(elasticClient, "cluster.materials_index", "cluster.type.materials", null);
        logger.info("同步采购品lastSyncTime:" + new DateTime(lastSyncTime).toString(SyncTimeUtil.DATE_TIME_PATTERN) + ",\n" + "syncTime:"
                + new DateTime().toString(SyncTimeUtil.DATE_TIME_PATTERN));
        syncMaterialsService(lastSyncTime);
    }

    private void syncMaterialsService(Timestamp lastSyncTime) {
        logger.info("2.1【开始】同步采购品");
        String countSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\t`corp_directory` \n" +
                "WHERE\n" +
                "\t update_time >?";

        String querySql = "select corp.ID as id,corp.company_id as companyId,CATALOG_ID as catalogId , CATALOG_NAME_PATH as catalogNamePath," +
                "CATALOG_ID_PATH as catalogIdPath,CODE as code,NAME as name,SPEC as spec,ABANDON as abandon,PCODE as pcode," +
                "PRODUCTOR as productor,UNITNAME as unitName,PRODUCING_ADDRESS as producingAddress,BRAND as brand,PURPOSE as purpose," +
                "MARKET_PRICE as marketPrice,SPECIALITY as speciality,SOURCE as source,tech_parameters as techParameters,DEMO as demo," +
                "unit_precision as unitPrecision,price_precision as pricePrecision,img_code as imgCode,corp.create_time as createTime," +
                "corp.update_time as updateTime,GROUP_CONCAT(org.org_id) as orgIdArr\n " +
                "from\n" +
                "(select * from corp_directory where update_time > ? LIMIT ?,?) corp \n " +
                "left join \n" +
                "corp_org_directory org on corp.ID = org.directory_id GROUP BY corp.ID";

        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        doSyncMaterialsData(uniregDataSource, countSql, querySql, params );
        logger.info("2.2【结束】同步招标项目成交供应商");
    }

    private void doSyncMaterialsData(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.info("执行countSql:{}, 参数params:{}, 共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);
                logger.info("执行querySql:{}, 参数 paramsToUse:{}, 总条数:{}", querySql, paramsToUse, mapList.size());
                //字段转换或添加字段
                refresh(mapList);
                batchInsert(mapList);
            }
        }
    }

    private void batchInsert(List<Map<String, Object>> mapList) {
//        System.out.println(mapList);
        if (!CollectionUtils.isEmpty(mapList)) {
            BulkRequestBuilder requestBuilder = elasticClient.getTransportClient().prepareBulk();
            mapList.forEach(map -> requestBuilder.add(elasticClient.getTransportClient().prepareIndex(
                    elasticClient.getProperties().getProperty("cluster.materials_index"),
                    elasticClient.getProperties().getProperty("cluster.type.materials")
                    , String.valueOf(map.get(ID))
            ).setSource(SyncTimeUtil.handlerDate(map))));

            BulkResponse bulkResponse = requestBuilder.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                logger.error(bulkResponse.buildFailureMessage());
            }
        }
    }

    /**
     *   //字段转换或添加字段
     * @param mapList
     */
    private void refresh(List<Map<String, Object>> mapList) {
        mapList.forEach(map -> {
            map.put(NAMENOTANALYZED,map.get(NAME));
            map.put(SyncTimeUtil.SYNC_TIME, SyncTimeUtil.getCurrentDate());
            String orgIdsStr = (String) map.get(ORGIDARR);
            Long[] orgIdArr = null;
            if(!TextUtils.isBlank(orgIdsStr)){
                try {
                    String[] orgIdStrArr = orgIdsStr.split(",");
                    orgIdArr = Arrays.stream(orgIdStrArr).map(orgIdSt -> Long.parseLong(orgIdSt)).collect(Collectors.toList()).toArray(new Long[orgIdStrArr.length]);
                    logger.info("..............SyncMaterialsDataJobHandler调用refresh的id:{},orgIdStr:{}",map.get(ID),orgIdsStr);
                } catch (Exception e) {
                    logger.error("SyncMaterialsDataJobHandler调用refresh转换orgIdArr时异常，异常原因:{}",e.getCause());
                }
            }
            map.put(ORGIDARR,orgIdArr);
        });
    }
    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/
}
