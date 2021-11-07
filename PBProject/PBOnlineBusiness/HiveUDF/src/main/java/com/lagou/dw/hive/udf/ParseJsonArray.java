/**
 * @Author: april
 * @Date: 3/11/21 2:38 PM
 * @Project: PBOnlineBusiness
 * @Product: IntelliJ IDEA
 * @Package: com.lagou.dw.hive.udf
 */

package com.lagou.dw.hive.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.parquet.Strings;
import org.junit.Test;

import java.util.ArrayList;

public class ParseJsonArray extends UDF {
    public ArrayList<String> evaluate(String jsonStr, String
            arrKey){
        if (Strings.isNullOrEmpty(jsonStr)) {
            return null;
        }

        try {
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            JSONArray jsonArray = jsonObject.getJSONArray(arrKey);
            ArrayList<String> results = new ArrayList<>();
            for (Object o : jsonArray) {
                results.add(o.toString());
            }
            return results;
        } catch (Throwable t) {
            return null;
        }
    }

    @Test
    public void testParse() {
        String jsonStr = "{\"id\": 2,\"ids\": [201,202,203,204],\"total_number\": 4}";
        String key = "ids";

        ArrayList<String> arrayList = evaluate(jsonStr, key);
        System.out.println(JSON.toJSONString(arrayList));
    }
}
