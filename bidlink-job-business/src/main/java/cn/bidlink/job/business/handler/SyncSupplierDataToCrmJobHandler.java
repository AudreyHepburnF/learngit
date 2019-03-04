package cn.bidlink.job.business.handler;

import cn.bidlink.job.common.utils.DBUtil;
import cn.bidlink.job.common.utils.SyncTimeUtil;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.annotation.JobHander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * @Date 2019/1/22
 */
@Service
@JobHander(value = "syncSupplierDataToCrmJobHandler")
public class SyncSupplierDataToCrmJobHandler extends JobHandler /*implements InitializingBean*/ {

    @Autowired
    @Qualifier(value = "uniregDataSource")
    private DataSource uniregDataSource;

    @Autowired
    @Qualifier(value = "crmDataSource")
    private DataSource crmDataSource;

    private Logger logger = LoggerFactory.getLogger(SyncSupplierDataToCrmJobHandler.class);

    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        logger.info("开始同步供应商数据到crm系统中");
        SyncTimeUtil.setCurrentDate();
        this.syncSupplierDataToCrm();
        logger.info("结束同步供应商数据到crm系统中");
        return ReturnT.SUCCESS;
    }

    private void syncSupplierDataToCrm() {
        String queryMaxLastSyncTime = "select max(sync_time) from t_reg_supplier";
        Timestamp lastSyncTime = (Timestamp) DBUtil.max(crmDataSource, queryMaxLastSyncTime, null);
        if (lastSyncTime == null || Objects.equals(lastSyncTime, SyncTimeUtil.GMT_TIME)) {
            lastSyncTime = new Timestamp(SyncTimeUtil.toStringDate("2019-01-18 14:24:00").getTime());
        }
        logger.info("同步供应商数据到crm系统,lastSyncTime:" + SyncTimeUtil.toDateString(lastSyncTime) + ",\n syncTime:" + SyncTimeUtil.currentDateToString());
        this.syncSupplierDataToCrmService(lastSyncTime);
    }

    private void syncSupplierDataToCrmService(Timestamp lastSyncTime) {
        String createCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND t.CREATE_TIME > ?\n";
        String createQuerySql = "SELECT\n" +
                "\tt.NAME,\n" +
                "\tt.COUNTRY,\n" +
                "\tt.AREA,\n" +
                "\tt.ADDRESS,\n" +
                "\tt.ZIP_CODE,\n" +
                "\tt.FUND,\n" +
                "\tt.FUNDUNIT,\n" +
                "\tt.BOSS_NAME,\n" +
                "\tCAST( t.MAIN_PRODUCT AS CHAR ) AS MAIN_PRODUCT,\n" +
                "\tDATE_FORMAT( t.CREATE_TIME, '%Y-%m-%d %H:%i:%S' ) AS CREATE_TIME,\n" +
                "\tt.TYPE,\n" +
                "\tt.WWW_STATION,\n" +
                "\tt.COMP_TYPE,\n" +
                "\tt.COMP_TYPE_STR,\n" +
                "\tt.INDUSTRY,\n" +
                "\tCAST( t.INDUSTRY_STR AS CHAR ) AS INDUSTRY_STR,\n" +
                "\tt.WEB_TYPE,\n" +
                "\tCAST( t.COMPANY_DESC AS CHAR ) AS COMPANY_DESC,\n" +
                "\tt.WORKPATTERN,\n" +
                "\tt.NAME_ENGLISH,\n" +
                "\tt.REG_NUMBER,\n" +
                "\tt.BIDAUTH_TIME,\n" +
                "\tt.BIDAUTH_STATUS,\n" +
                "\tu.SEX,\n" +
                "\tu.TEL,\n" +
                "\tu.MOBILE,\n" +
                "\tu.LOGIN_NAME,\n" +
                "\tu.FAX,\n" +
                "\tu.name as link_man,\n" +
                "\tu.EMAIL \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND t.CREATE_TIME > ?\n" +
                "ORDER BY\n" +
                "\tt.CREATE_TIME ASC  limit ?,?";
        this.doSyncSupplierDataToCrmService(uniregDataSource, createCountSql, createQuerySql, Collections.singletonList(lastSyncTime));

        String updateCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND t.UPDATE_TIME > ?\n";
        String updateQuerySql = "SELECT\n" +
                "\tt.NAME,\n" +
                "\tt.COUNTRY,\n" +
                "\tt.AREA,\n" +
                "\tt.ADDRESS,\n" +
                "\tt.ZIP_CODE,\n" +
                "\tt.FUND,\n" +
                "\tt.FUNDUNIT,\n" +
                "\tt.BOSS_NAME,\n" +
                "\tCAST( t.MAIN_PRODUCT AS CHAR ) AS MAIN_PRODUCT,\n" +
                "\tDATE_FORMAT( t.CREATE_TIME, '%Y-%m-%d %H:%i:%S' ) AS CREATE_TIME,\n" +
                "\tt.TYPE,\n" +
                "\tt.WWW_STATION,\n" +
                "\tt.COMP_TYPE,\n" +
                "\tt.COMP_TYPE_STR,\n" +
                "\tt.INDUSTRY,\n" +
                "\tCAST( t.INDUSTRY_STR AS CHAR ) AS INDUSTRY_STR,\n" +
                "\tt.WEB_TYPE,\n" +
                "\tCAST( t.COMPANY_DESC AS CHAR ) AS COMPANY_DESC,\n" +
                "\tt.WORKPATTERN,\n" +
                "\tt.NAME_ENGLISH,\n" +
                "\tt.REG_NUMBER,\n" +
                "\tt.BIDAUTH_TIME,\n" +
                "\tt.BIDAUTH_STATUS,\n" +
                "\tu.SEX,\n" +
                "\tu.TEL,\n" +
                "\tu.MOBILE,\n" +
                "\tu.LOGIN_NAME,\n" +
                "\tu.FAX,\n" +
                "\tu.name as link_man,\n" +
                "\tu.EMAIL \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND t.UPDATE_TIME > ?\n" +
                "ORDER BY\n" +
                "\tt.CREATE_TIME ASC  limit ?,?";
        this.doSyncSupplierDataToCrmService(uniregDataSource, updateCountSql, updateQuerySql, Collections.singletonList(lastSyncTime));

        String updateUserCountSql = "SELECT\n" +
                "\tcount( 1 ) \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND u.UPDATE_TIME > ?\n";
        String updateUserQuerySql = "SELECT\n" +
                "\tt.NAME,\n" +
                "\tt.COUNTRY,\n" +
                "\tt.AREA,\n" +
                "\tt.ADDRESS,\n" +
                "\tt.ZIP_CODE,\n" +
                "\tt.FUND,\n" +
                "\tt.FUNDUNIT,\n" +
                "\tt.BOSS_NAME,\n" +
                "\tCAST( t.MAIN_PRODUCT AS CHAR ) AS MAIN_PRODUCT,\n" +
                "\tDATE_FORMAT( t.CREATE_TIME, '%Y-%m-%d %H:%i:%S' ) AS CREATE_TIME,\n" +
                "\tt.TYPE,\n" +
                "\tt.WWW_STATION,\n" +
                "\tt.COMP_TYPE,\n" +
                "\tt.COMP_TYPE_STR,\n" +
                "\tt.INDUSTRY,\n" +
                "\tCAST( t.INDUSTRY_STR AS CHAR ) AS INDUSTRY_STR,\n" +
                "\tt.WEB_TYPE,\n" +
                "\tCAST( t.COMPANY_DESC AS CHAR ) AS COMPANY_DESC,\n" +
                "\tt.WORKPATTERN,\n" +
                "\tt.NAME_ENGLISH,\n" +
                "\tt.REG_NUMBER,\n" +
                "\tt.BIDAUTH_TIME,\n" +
                "\tt.BIDAUTH_STATUS,\n" +
                "\tu.SEX,\n" +
                "\tu.TEL,\n" +
                "\tu.MOBILE,\n" +
                "\tu.LOGIN_NAME,\n" +
                "\tu.FAX,\n" +
                "\tu.name as link_man,\n" +
                "\tu.EMAIL \n" +
                "FROM\n" +
                "\tt_reg_company t\n" +
                "\tLEFT JOIN t_reg_user u ON u.COMPANY_ID = t.id \n" +
                "WHERE\n" +
                "\tt.TYPE in (12,13) and (t.is_test = 0 or t.is_test is null and t.name not like \"%必联%\" and t.name not like \"%测试%\")\n" +
                "\tAND u.UPDATE_TIME > ?\n" +
                "ORDER BY\n" +
                "\tt.CREATE_TIME ASC  limit ?,?";
        this.doSyncSupplierDataToCrmService(uniregDataSource, updateUserCountSql, updateUserQuerySql, Collections.singletonList(lastSyncTime));
    }

    private void doSyncSupplierDataToCrmService(DataSource uniregDataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(uniregDataSource, countSql, params);
        logger.info("执行countSql:{},params:{},count:{}", countSql, params, count);
        for (int i = 0; i < count; i += pageSize) {
            List<Object> paramsToUse = this.appendToParams(params, i);
            List<Map<String, Object>> mapList = DBUtil.query(uniregDataSource, querySql, paramsToUse);
            logger.info("执行querySql:{},params:{},size:{}", querySql, paramsToUse, mapList.size());
            this.insertData(mapList);
        }
    }

    private void insertData(List<Map<String, Object>> mapList) {
        if (!CollectionUtils.isEmpty(mapList)) {
            StringBuilder sqlBuilder = this.sqlBuilder(mapList);
            List<Object> insertParams = this.builderParams(mapList, sqlBuilder);
            DBUtil.execute(crmDataSource, sqlBuilder.toString(), insertParams);
        }
    }

    private List<Object> builderParams(List<Map<String, Object>> mapList, StringBuilder sqlBuilder) {
        List<Object> insertParams = new ArrayList<>();
        int listIndex = 0;
        for (Map<String, Object> map : mapList) {
            if (listIndex > 0) {
                sqlBuilder.append(", (");
            } else {
                sqlBuilder.append(" ( ");
            }
            int size = map.size() + 1;
            for (int j = 0; j < size; j++) {
                if (j > 0) {
                    sqlBuilder.append(" , ? ");
                } else {
                    sqlBuilder.append(" ? ");
                }
            }
            listIndex++;
            sqlBuilder.append(" ) ");
            insertParams.addAll(map.values());
            insertParams.add(SyncTimeUtil.getCurrentDate());
        }
        return insertParams;
    }

    private StringBuilder sqlBuilder(List<Map<String, Object>> mapList) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO t_reg_supplier (");
        Set<String> columnsSet = mapList.get(0).keySet();
        int columnIndex = 0;
        for (String columnName : columnsSet) {
            if (columnIndex > 0) {
                sqlBuilder.append(",").append(columnName);
            } else {
                sqlBuilder.append(columnName);
            }
            columnIndex++;
        }
        // 添加同步时间字段
        sqlBuilder.append(", sync_time) VALUES");
        return sqlBuilder;
    }

//    @Override
//    public void afterPropertiesSet() throws Exception {
//        execute();
//    }
}
