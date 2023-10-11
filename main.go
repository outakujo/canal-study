package main

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/withlin/canal-go/client"
	pbe "github.com/withlin/canal-go/protocol/entry"
)

func main() {
	connector := client.NewSimpleCanalConnector("localhost", 11111, "",
		"", "example", 60000, 60*60*1000)
	err := connector.Connect()
	if err != nil {
		panic(err)
	}
	// https://github.com/alibaba/canal/wiki/AdminGuide
	//mysql 数据解析关注的表，Perl正则表达式.
	//
	//多个正则之间以逗号(,)分隔，转义符需要双斜杠(\\)
	//
	//常见例子：
	//
	//  1.  所有表：.*   or  .*\\..*
	//	2.  canal schema下所有表： canal\\..*
	//	3.  canal下的以canal打头的表：canal\\.canal.*
	//	4.  canal schema下的一张表：canal\\.test1
	//  5.  多个规则组合使用：canal\\..*,mysql.test1,mysql.test2 (逗号分隔)
	err = connector.Subscribe(".*\\..*")
	if err != nil {
		panic(err)
	}

	for {
		message, err := connector.Get(100, nil, nil)
		if err != nil {
			panic(err)
		}
		batchId := message.Id
		if batchId == -1 || len(message.Entries) <= 0 {
			time.Sleep(time.Second)
			continue
		}
		err = printEntry(message.Entries)
		if err != nil {
			panic(err)
		}
	}
}

func printEntry(entrys []pbe.Entry) error {
	for _, entry := range entrys {
		if entry.GetEntryType() == pbe.EntryType_TRANSACTIONBEGIN ||
			entry.GetEntryType() == pbe.EntryType_TRANSACTIONEND {
			continue
		}
		rowChange := new(pbe.RowChange)
		err := proto.Unmarshal(entry.GetStoreValue(), rowChange)
		if err != nil {
			return err
		}
		if rowChange == nil {
			continue
		}
		fmt.Println("sql:", rowChange.GetSql())
		eventType := rowChange.GetEventType()
		header := entry.GetHeader()
		fmt.Println(fmt.Sprintf("binlog[%s : %d],name[%s,%s], eventType: %s",
			header.GetLogfileName(), header.GetLogfileOffset(), header.GetSchemaName(),
			header.GetTableName(), header.GetEventType()))
		for _, rowData := range rowChange.GetRowDatas() {
			if eventType == pbe.EventType_DELETE {
				printColumn(rowData.GetBeforeColumns(), false)
			} else if eventType == pbe.EventType_INSERT {
				printColumn(rowData.GetAfterColumns(), true)
			} else {
				printColumn(rowData.GetAfterColumns(), true)
			}
		}
	}
	return nil
}

func printColumn(columns []*pbe.Column, update bool) {
	for _, col := range columns {
		if update && !col.GetUpdated() {
			continue
		}
		fmt.Println(fmt.Sprintf("%s : %s  update= %t", col.GetName(), col.GetValue(), col.GetUpdated()))
	}
}
