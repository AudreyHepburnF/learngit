package cn.bidlink.job.business.utils;

import cn.bidlink.job.common.utils.DBUtil;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AreaUtil {
    private static String queryAreaInfoTemplate = "SELECT\n"
                                                  + "   t3.ID AS id,\n"
                                                  + "   t3.AREA AS area,\n"
                                                  + "   t3.CODE AS code,\n"
                                                  + "   t3.CITY AS city,\n"
                                                  + "   tcrd.`VALUE` AS county\n"
                                                  + "FROM\n"
                                                  + "   (\n"
                                                  + "      SELECT\n"
                                                  + "         t2.ID, t2.AREA, t2.CODE,t2.COUNTY, tcrd.`VALUE` AS CITY\n"
                                                  + "      FROM\n"
                                                  + "         (\n"
                                                  + "            SELECT\n"
                                                  + "               t1.ID, t1.CITY, t1.COUNTY, tcrd.`KEY` AS CODE, tcrd.`VALUE` AS AREA\n"
                                                  + "            FROM\n"
                                                  + "               (SELECT ID, COUNTRY, AREA, CITY, COUNTY FROM t_reg_company WHERE ID IN (%s) AND COUNTRY IS NOT NULL) t1\n"
                                                  + "            JOIN t_reg_center_dict tcrd ON t1.AREA = tcrd.`KEY`\n"
                                                  + "            WHERE\n"
                                                  + "               tcrd.TYPE = 'country'\n"
                                                  + "         ) t2\n"
                                                  + "      LEFT JOIN t_reg_center_dict tcrd ON t2.CITY = tcrd.`KEY`\n"
                                                  + "      WHERE\n"
                                                  + "         tcrd.TYPE = 'country' OR tcrd.TYPE IS NULL\n"
                                                  + "   ) t3\n"
                                                  + "LEFT JOIN t_reg_center_dict tcrd ON t3.COUNTY = tcrd.`KEY`\n"
                                                  + "WHERE\n"
                                                  + "   tcrd.TYPE = 'country' OR tcrd.TYPE IS NULL";

    public static Map<Long, AreaInfo> queryAreaInfo(DataSource dataSource, Set<Long> companyIds) {
        String queryAreaSql = String.format(queryAreaInfoTemplate, StringUtils.collectionToCommaDelimitedString(companyIds));
        List<Map<String, Object>> query = DBUtil.query(dataSource, queryAreaSql, null);
        Map<Long, AreaInfo> areaMap = new HashMap<>();
        for (Map<String, Object> map : query) {
            Object area = map.get("area");
            Object city = map.get("city");
            Object county = map.get("county");
            String areaStr = "";
            if (area != null) {
                areaStr += area;
            }

            if (city != null) {
                areaStr += city;
            }

            if (county != null) {
                areaStr += county;
            }
            // 特殊处理
            if (areaStr != null && areaStr.indexOf("市辖区") > -1) {
                areaStr = areaStr.replace("市辖区", "");
            }

            AreaInfo areaInfo = new AreaInfo(areaStr);
            // 处理省、直辖市
            String code = (String) map.get("code");
            if (code != null && code.length() > 2) {
                areaInfo.region = RegionUtil.regionMap.get(code.substring(0, 2));
            }
            areaMap.put((Long) map.get("id"), areaInfo);
        }
        return areaMap;
    }

    public static class AreaInfo {
        private String areaStr;
        private String region;

        public String getAreaStr() {
            return areaStr;
        }

        public String getRegion() {
            return region;
        }

        public AreaInfo(String areaStr) {
            this.areaStr = areaStr;
        }

        public AreaInfo(String areaStr, String region) {
            this.areaStr = areaStr;
            this.region = region;
        }

        @Override
        public String toString() {
            return "RegionCla{" +
                   "areaStr='" + areaStr + '\'' +
                   ", region='" + region + '\'' +
                   '}';
        }
    }
}