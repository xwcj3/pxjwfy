package com.ccj.pxj;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import java.net.InetSocketAddress;
import java.util.List;

public class CanalTemo {
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector((new InetSocketAddress("hadoop54", 11111)), " example", "", "");
        while (true){
           canalConnector.connect();
            Message message = canalConnector.get(5);
            canalConnector.subscribe("gmall2019.test");
            if(message.getEntries().size()==0){
                try{
                    System.out.println("没有数据，休息5秒");
                    Thread.sleep(5000);
                }catch (Exception e){
                    e.printStackTrace();
                }

            }else {
                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {
                    CanalEntry.EntryType entryType = entry.getEntryType();
                    if(entryType==CanalEntry.EntryType.TRANSACTIONBEGIN && entryType==CanalEntry.EntryType.TRANSACTIONEND){
                        continue;
                    }
                    String tableName = entry.getHeader().getTableName();
                    //把entry转化成rowchange
                    CanalEntry.RowChange  rowChange=null;
                    try {
                        rowChange=CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                   CanalHandler.handle(tableName,rowChange.getEventType(),rowDatasList);

                }
            }

        }
    }
}
