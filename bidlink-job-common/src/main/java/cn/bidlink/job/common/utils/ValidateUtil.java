package cn.bidlink.job.common.utils;

import java.util.Map;
import java.util.Objects;

/**
 * @author <a href="mailto:zhihuizhou@ebnew.com">wisdom</a>
 * @version Ver 1.0
 * @description:企业数据检查
 * @Date 2018/10/25
 */
public class ValidateUtil {

    // 采购商
    private static final Integer PURCHASER = 12;

    // 供应商
    private static final Integer SUPPLIER = 13;

    // 完善
    private static final Integer COMPLETE = 1;

    // 不完善
    private static final Integer NOT_COMPLETE = 0;

    // 限制次数
    private static final Integer LIMIT_NUM = 3;

    /**
     * 检查数据完整性 1:完整 0:不完整
     *
     * @param map
     * @param companyType
     * @return
     */
    public static Integer checkDataComplete(Map<String, Object> map, Integer companyType) {
        Integer isEmptyNum = 0;
        if (Objects.equals(PURCHASER, companyType)) {
            // 采购商校验 行业 地区 企业性质
            if (Objects.isNull(map.get("industryStr"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("areaStr"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("compTypeStr"))) {
                isEmptyNum += 1;
            }
        } else if (Objects.equals(SUPPLIER, companyType)) {
            // 供应商校验 注册资本 主营产品 经营模式 所属行业 所在地区
            if (Objects.isNull(map.get("fund"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("mainProduct"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("workPattern"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("industryStr"))) {
                isEmptyNum += 1;
            }
            if (Objects.isNull(map.get("areaStr"))) {
                isEmptyNum += 1;
            }
        }
        if (isEmptyNum >= LIMIT_NUM) {
            return NOT_COMPLETE;
        } else {
            return COMPLETE;
        }
    }
}
