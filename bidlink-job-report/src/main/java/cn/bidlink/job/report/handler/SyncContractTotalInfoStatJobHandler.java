package cn.bidlink.job.report.handler;


import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;


/**
 * @author <a href="mailto:libingwang@ebnew.com">libingwang</a>
 * @version Ver 1.0
 * @description:合同总项报表统计
 * @Date 2018/1/8
 */
@Service
@JobHander("syncContractTotalInfoStatJobHandler")
public class SyncContractTotalInfoStatJobHandler extends SyncJobHandler  /*implements InitializingBean*/ {
    private Logger logger = LoggerFactory.getLogger(SyncPurchaseTradingVolumeStatJobHandler.class);

    private String CREATE_USER_NAME = "create_user_name";
    private String NAME = "name";
    private String ID = "id";
    private String COMPANY_ID = "company_id";
    private String USER_ID = "user_id";
    private String CONTRACT_ID = "contract_id";
    private String PURCHASER_COMPANY_SIGN_NAME = "purchaser_company_sign_name";
    private String SUPPLIER_COMPANY_SIGN_NAME = "supplier_company_sign_name";
    @Override
    protected String getTableName() {
        return "contract_total_info_stat";
    }

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();

        logger.info("同步合同总项报表统计开始");
        syncContractTotalInfo();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步合同总项报表统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncContractTotalInfo() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步合同总项报表统计lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "count(1)\n" +
                "FROM\n" +
                " con_contract c\n" +
                "where c.create_time > ?\n"
                ;

        String querySql = "SELECT\n" +
                " c.id AS contract_id,\n" +
                " c.company_id,\n" +
                " c.creator_id AS user_id,\n" +
                " c.create_time,\n" +
                " c. CODE,\n" +
                " c.serial_code,\n" +
                " c. NAME AS contract_name,\n" +
                " (\n" +
                "  SELECT\n" +
                "   sum(\n" +
                "    corp.univalence * corp.number\n" +
                "   )\n" +
                "  FROM\n" +
                "   con_corpore corp\n" +
                "  WHERE\n" +
                "   c.id = corp.contract_id\n" +
                "  AND c.company_id = corp.company_id\n" +
                " ) AS total_price,\n" +
                " (\n" +
                "  CASE c. STATUS\n" +
                "  WHEN 1 THEN\n" +
                "   '新建'\n" +
                "  WHEN 2 THEN\n" +
                "   '待确认'\n" +
                "  WHEN 3 THEN\n" +
                "   '已确认'\n" +
                "  WHEN 4 THEN\n" +
                "   '已生效'\n" +
                "  WHEN 5 THEN\n" +
                "   '废弃'\n" +
                "  WHEN 6 THEN\n" +
                "   '已拒绝'\n" +
                "  END\n" +
                " ) AS STATUS,\n" +
                " c.FILIALE_CODE,\n" +
                " c.department_code,\n" +
                " c.filiale_name,\n" +
                " c.sign_time,\n" +
                " c.classification_code,\n" +
                " c.classification_name,\n" +
                " c.project_insdustry_code,\n" +
                " c.project_insdustry_name,\n" +
                " (\n" +
                "  CASE c.has_agreement\n" +
                "  WHEN 1 THEN\n" +
                "   '协议'\n" +
                "  WHEN 2 THEN\n" +
                "   '非协议'\n" +
                "  END\n" +
                " ) AS has_agreement\n" +
                "FROM\n" +
                " con_contract c\n" +
                "where c.create_time > ?\n"+
                "\t\tLIMIT ?,?\n";
        ArrayList<Object> params = new ArrayList<>();
        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);


    }

    @Override
    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                // 添加catalog_id
                appendCreateUserName(mapList);
                appendPurchaserAndSupplierCompanySignName(mapList);
                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());
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
     * 添加用户姓名
     *
     * @param mapList
     */
    private void appendCreateUserName(List<Map<String, Object>> mapList) {
        // 查找所有的user_id,company_id
        Map<Object, Object> map = new HashMap<>();
        for (Map<String, Object> mapAttr : mapList) {
            map.put(mapAttr.get(USER_ID), mapAttr.get(COMPANY_ID));
        }

        // 从corp_directorys找catalog_id
        StringBuilder selectCreateUserNameSqlBuilder = new StringBuilder();
        selectCreateUserNameSqlBuilder.append("SELECT\n" +
                "\tid,\n" +
                "name \n" +
                "FROM\n" +
                "\treg_user \n" +
                "WHERE " );

        int conditionIndex = 0;
        for (Map.Entry<Object, Object> attr : map.entrySet()) {
            if(conditionIndex > 0) {
                selectCreateUserNameSqlBuilder.append(" or").append("(id = ").append(attr.getKey())
                        .append(" AND company_id = ").append(attr.getValue()+")");
            }else{
                selectCreateUserNameSqlBuilder.append("(id = ").append(attr.getKey())
                        .append(" AND company_id = ").append(attr.getValue()+")");
            }
            conditionIndex++;
        }

        // 将查出来的数据封装成map
        final Map<Long, String> catalogIdMap = DBUtil.query(ycDataSource, selectCreateUserNameSqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<Long, String>>() {
            @Override
            public Map<Long, String> execute(ResultSet resultSet) throws SQLException {
                HashMap<Long, String> map = new HashMap<>();
                while (resultSet.next()) {
                    long userId = resultSet.getLong(ID);
                    String createUserName = resultSet.getString(NAME);
                    map.put(userId,createUserName);
                }
                return map;
            }
        });
        // 遍历maplist, set
        for (Map<String, Object> mapAttr : mapList) {
            Long key = (Long) mapAttr.get(USER_ID);
            String createUserName = catalogIdMap.get(key);
            if (createUserName != null) {
                mapAttr.put(CREATE_USER_NAME, createUserName);
            } else {
                mapAttr.put(CREATE_USER_NAME, null);
            }
        }

    }

    /**
     * 添加purchaser_company_sign_name,supplier_company_sign_name
     *
     * @param mapList
     */
    private void appendPurchaserAndSupplierCompanySignName(List<Map<String, Object>> mapList) {
        // 查找所有的contract_id,company_id
        Map<Object, Object> mapString = new HashMap<>();
        for (Map<String, Object> mapAttr : mapList) {
            mapString.put(mapAttr.get(CONTRACT_ID), mapAttr.get(COMPANY_ID));
        }

        // 从con_tract_tail找purchaser_company_sign_name,supplier_company_sign_name
        StringBuilder selectSignNameSqlBuilder = new StringBuilder();
        selectSignNameSqlBuilder.append("SELECT\n" +
                "\tCONTRACT_ID,\n" +
                "\tCOMPANY_ID,\n" +
                "\tPURCHASER_COMPANY_SIGN_NAME,\n" +
                "\tSUPPLIER_COMPANY_SIGN_NAME \n" +
                "FROM\n" +
                "\tcon_contract_tail \n" +
                "WHERE \n" );

        int conditionIndex = 0;
        for (Map.Entry<Object, Object> attr : mapString.entrySet()) {
            if(conditionIndex > 0) {
                selectSignNameSqlBuilder.append(" or").append("(contract_id = ").append(attr.getKey())
                        .append(" AND company_id = ").append(attr.getValue()+")");
            }else{
                selectSignNameSqlBuilder.append("(contract_id = ").append(attr.getKey())
                        .append(" AND company_id = ").append(attr.getValue()+")");
            }
            conditionIndex++;
        }

        // 将查出来的数据封装成map
        final Map<String, StringBuilder> catalogIdMap = DBUtil.query(ycDataSource, selectSignNameSqlBuilder.toString(), null, new DBUtil.ResultSetCallback<Map<String, StringBuilder>>() {
            @Override
            public Map<String, StringBuilder> execute(ResultSet resultSet) throws SQLException {
                HashMap<String, StringBuilder> map = new HashMap<>();
                while (resultSet.next()) {
                    String contractId = resultSet.getString(CONTRACT_ID);
                    String purchaserCompanySignName = StringUtils.isEmpty(resultSet.getString(PURCHASER_COMPANY_SIGN_NAME))?"0":resultSet.getString(PURCHASER_COMPANY_SIGN_NAME);
                    String supplierCompanySignName = StringUtils.isEmpty(resultSet.getString(SUPPLIER_COMPANY_SIGN_NAME))?"0":resultSet.getString(SUPPLIER_COMPANY_SIGN_NAME);
                    StringBuilder stringBuilder = new StringBuilder();
                    stringBuilder.append(purchaserCompanySignName+",").append(supplierCompanySignName);
                    map.put(contractId,stringBuilder);
                }
                return map;
            }
        });
        // 遍历maplist, set
        for (Map<String, Object> mapAttr : mapList) {
            Long key = (Long) mapAttr.get(CONTRACT_ID);
            if(catalogIdMap.containsKey(key.toString())){
                String purchaserAndSupplierCompanySignName = catalogIdMap.get(key.toString())==null?"0":catalogIdMap.get(key.toString()).toString();
                String[] split = purchaserAndSupplierCompanySignName.split(",");
                if(split != null && split.length>0){

                    if (!split[0].equals("0")) {
                        mapAttr.put(PURCHASER_COMPANY_SIGN_NAME, split[0]);
                    } else {
                        mapAttr.put(PURCHASER_COMPANY_SIGN_NAME, null);
                    }
                    if(!split[1].equals("0")){
                        mapAttr.put(SUPPLIER_COMPANY_SIGN_NAME, split[1]);
                    }else{
                        mapAttr.put(SUPPLIER_COMPANY_SIGN_NAME, null);
                    }
                }else{
                    mapAttr.put(PURCHASER_COMPANY_SIGN_NAME, null);
                    mapAttr.put(SUPPLIER_COMPANY_SIGN_NAME, null);
                }
            }else{
                mapAttr.put(PURCHASER_COMPANY_SIGN_NAME, null);
                mapAttr.put(SUPPLIER_COMPANY_SIGN_NAME, null);
            }

        }

    }

    /*@Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }*/

}
