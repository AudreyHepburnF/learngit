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

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:jiacaisu@ebnew.com">jiacaisu</a>
 * @version Ver 1.0
 * @description 合同概况统计
 * @Date 2018/1/4
 */
@Service
@JobHander("syncContractOverviewStatJobHandler")
public class SyncContractOverviewStatJobHandler extends SyncJobHandler implements InitializingBean{

    private Logger logger = LoggerFactory.getLogger(SyncContractOverviewStatJobHandler.class);

    @Override
    protected String getTableName() {
        return "contract_overview_stat";
    }



    @Override
    public ReturnT<String> execute(String... strings) throws Exception {
        // 当前时间和线程绑定
        SyncTimeUtil.setCurrentDate();

        logger.info("同步合同概况统计开始");
        //合同概况统计报表
        syncContractOverview();
        // 记录同步时间
        updateSyncLastTime();
        logger.info("同步合同概况统计结束");
        return ReturnT.SUCCESS;
    }

    private void syncContractOverview() {

        Date lastSyncTime = getLastSyncTime();
        logger.info("同步招标整体跟踪进行中-统计开始lastSyncTime: " + new DateTime(lastSyncTime).toString("yyyy-MM-dd HH:mm:ss"));

        String countSql = "SELECT\n" +
                "count(1)\n" +
                "FROM\n" +
                "\tcon_contract c\n" +
                "INNER JOIN con_corpore con ON c.id = con.CONTRACT_ID\n" +
                "AND c.company_id = con.COMPANY_ID";


        String querySql = "SELECT\n" +
                "\tc. STATUS,\n" +
                "\tc.COMPANY_ID,\n" +
                "\tc.supplier_id,\n" +
                "\tcon.number,\n" +
                "\tcon.UNIVALENCE,\n" +
                "\tc.create_time,\n" +
                "\tcon.CONTRACT_ID\n" +
                "FROM\n" +
                "\tcon_contract c\n" +
                "INNER JOIN con_corpore con ON c.id = con.CONTRACT_ID\n" +
                "AND c.company_id = con.COMPANY_ID\n"+
                "ORDER BY c.id\n" +
                "\t LIMIT ?,? \n";
        ArrayList<Object> params = new ArrayList<>();
//        params.add(lastSyncTime);
        sync(ycDataSource, countSql, querySql, params);
    }



    @Override
    public void afterPropertiesSet() throws Exception {
        execute();
    }

    protected void sync(DataSource dataSource, String countSql, String querySql, List<Object> params) {
        long count = DBUtil.count(dataSource, countSql, params);
        logger.debug("执行countSql : {}, params : {}，共{}条", countSql, params, count);
        if (count > 0) {
            for (long i = 0; i < count; i += pageSize) {
                // 添加分页查询参数
                List<Object> paramsToUse = appendToParams(params, i);
                List<Map<String, Object>> mapList = DBUtil.query(dataSource, querySql, paramsToUse);

                // 添加供应商类型
                appendSupplierType(mapList);

                logger.debug("执行querySql : {}, paramsToUse : {}，共{}条", querySql, paramsToUse, mapList.size());
                // 生成insert sql, 参数
                if (!CollectionUtils.isEmpty(mapList)) {
                    System.out.println(mapList);
                }
            }
        }
    }

    private void appendSupplierType(List<Map<String, Object>> mapList) {

    }


}
