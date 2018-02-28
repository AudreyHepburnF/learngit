package cn.bidlink.job.business.contants;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author : <a href="mailto:zikaifeng@ebnew.com">冯子恺</a>
 * @version : Ver 1.0
 * @description :
 * @date : 2018/2/27
 */
public abstract class Regions {
    // 每个省，直辖市对应的地区
    public static final Map<String, String> regionMap;

    static {
        Map<String, String> map = new HashMap<>(64);
        /* 东北地区 */
        // 辽宁
        map.put("21", "东北地区");
        // 吉林
        map.put("22", "东北地区");
        // 黑龙江
        map.put("23", "东北地区");
        /* 西北地区 */
        // 陕西
        map.put("61", "西北地区");
        // 甘肃
        map.put("62", "西北地区");
        // 青海
        map.put("63", "西北地区");
        // 宁夏
        map.put("64", "西北地区");
        // 新疆
        map.put("65", "西北地区");
        /* 华北 */
        // 北京
        map.put("11", "华北地区");
        // 天津
        map.put("12", "华北地区");
        // 河北
        map.put("13", "华北地区");
        // 山西
        map.put("14", "华北地区");
        // 内蒙古
        map.put("15", "华北地区");
        /* 华中 */
        // 江西
        map.put("36", "华中地区");
        // 河南
        map.put("41", "华中地区");
        // 湖北
        map.put("42", "华中地区");
        // 湖南
        map.put("43", "华中地区");
        /* 华东 */
        // 上海
        map.put("31", "华东地区");
        // 江苏
        map.put("32", "华东地区");
        // 浙江
        map.put("33", "华东地区");
        // 安徽
        map.put("34", "华东地区");
        // 福建
        map.put("35", "华东地区");
        // 山东
        map.put("37", "华东地区");
        /* 华南 */
        // 广东
        map.put("44", "华南地区");
        // 广西
        map.put("45", "华南地区");
        // 海南
        map.put("46", "华南地区");
        /* 西南 */
        // 重庆
        map.put("50", "西南地区");
        // 四川
        map.put("51", "西南地区");
        // 贵州
        map.put("52", "西南地区");
        // 云南
        map.put("53", "西南地区");
        // 西藏
        map.put("54", "西南地区");
        /* 特别行政区 */
        // 台湾
        map.put("71", "特别行政区");
        // 香港
        map.put("81", "特别行政区");
        // 澳门
        map.put("82", "特别行政区");

        regionMap = Collections.unmodifiableMap(map);
    }
}
