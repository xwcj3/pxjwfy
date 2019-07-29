package com.ccj.pxj;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.ccj.pxj.GmallConstant;
import com.ccj.pxj.MyKafkaSender;
import com.google.common.base.CaseFormat;
import com.alibaba.fastjson.JSON;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanalHandler {
    public static  void handle(String tableName, CanalEntry.EventType eventType,List<CanalEntry.RowData> rowDatasList){
        if(eventType== CanalEntry.EventType.INSERT&&"order_info".equals(tableName)){
            for (CanalEntry.RowData rowData : rowDatasList) {
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

                Map orderMap=new HashMap();
                for (CanalEntry.Column column : afterColumnsList) {
                    // System.out.println("columnName:"+column.getName());
                    // System.out.println("columnValue:"+column.getValue());
                    String propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, column.getName());
                    orderMap.put(propertyName,column.getValue());
                    System.out.println(JSON.toJSONString(orderMap));

                }
               // MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, JSON.toJSONString(orderMap));
            }
        }
    }
}
