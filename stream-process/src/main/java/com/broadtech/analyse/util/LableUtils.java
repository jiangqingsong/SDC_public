package com.broadtech.analyse.util;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author mingcong.li01@hand-china.com
 * @version 1.0
 * @ClassName LableUtils
 * @description
 * @date 2020/6/10 15:54
 * @since JDK 1.8
 */

public class LableUtils {
    /**
     * labelMatch :
     *
     * @author mingcong.li01@hand-china.com
     * @version 1.0
     * @date 2020/6/10 17:20
     * @param matchStr	匹配的型号字段
     * @param labelData	 传入的数据库里的标签库信息
     * @param keyData	传入的数据库里的关键字信息
     * @return java.util.List<java.util.Map<java.lang.String,java.lang.Object>>
     * @since JDK 1.8
     */
    public static List<Map<String,Object>> labelMatch(String matchStr,
                                                      List<Map<String,Object>> labelData,
                                                      List<Map<String,Object>> keyData){
        if(null == matchStr || null == labelData ||
                null == keyData||labelData.size() == 0
                || keyData.size() == 0) {
            return null;
        }

        List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
        Map<String,Object> obj = null;
        List<Map<String,Object>> temp = null;
        for(Map<String,Object> keyword : keyData) {
            obj = new HashMap<String, Object>();
            temp = new ArrayList<Map<String,Object>>();
            obj.put("assetStr", matchStr);
            if((matchStr+" ").contains(keyword.get("keyword")+"")) {
                temp = labelData.stream().filter(x -> (x.get("id")+"")
                        .equals(keyword.get("label_id")+"")).collect(Collectors.toList());
                if(null == temp || temp.size() == 0) {
                    return null;
                }

                obj.put("label_1", temp.get(0).get("label_1"));
                obj.put("label_2", temp.get(0).get("label_2"));
            }

            result.add(obj);
        }
        result = result.stream().filter(x -> null == x.get("label_1")||StringUtils.isEmpty(x.get("label_1")+"")).collect(Collectors.toList());
        return delRepeat(result);
    }

    public static List<Map<String,Object>> delRepeat(List<Map<String,Object>> list) {
        List<Map<String,Object>> myList = list.stream().distinct().collect(Collectors.toList());
        return myList ;
    }
}
