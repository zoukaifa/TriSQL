using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using Trinity.Core.Lib;
using System.Threading;

namespace TriSQLApp
{
    class DatabaseServer : DatabaseServerBase
    {
        /// <summary>
        /// 查询数据库信息
        /// </summary>
        /// <param name="request"></param>
        /// <param name="response"></param>
        struct RowMessage
        {
            public List<long> cellId;
            public List<int> types;
            public List<int> usedIndex;
            public Condition con;
            public List<List<long>> result;
        }
        public override void GetDatabaseHandler(GetDatabaseMessageReader request, GetDatabaseResponseWriter response)
        {
            long cellId = request.databaseId;
            using (var db = Global.LocalStorage.UseDatabaseCell(cellId))
            {
                response.tableIdList = db.tableIdList;
                response.tableNameList = db.tableNameList;
            }
        }

        public override void GetElementHandler(GetElementMessageReader request, GetElementResponseWriter response)
        {
            ElementCell ec = Global.LocalStorage.UseElementCell(request.cellId);
            response.ele = FieldType.getElement(ec);
        }

        public override void GetRowHandler(GetRowMessageReader request, GetRowResponseWriter response)
        {
            List<long> eleIds = request.cellIds;
            response.row = new List<Element>();
            foreach (long eleId in eleIds)
            {
                int serverId = Global.CloudStorage.GetServerIdByCellId(eleId);
                if (serverId == Global.MyServerId)  //在本服务器上
                {
                    ElementCell ec = Global.LocalStorage.UseElementCell(eleId);
                    response.row.Add(FieldType.getElement(ec));
                }
                else
                {
                    Element ec = Global.CloudStorage.GetElementToDatabaseServer(
                        serverId, new GetElementMessageWriter(eleId)).ele;
                    response.row.Add(ec);
                }
            }
        }

        public override void GetTableHandler(GetTableMessageReader request, GetTableResponseWriter response)
        {
            using (var thcell = Global.LocalStorage.UseTableHeadCell(request.tableId))
            {
                response.columnNameList = thcell.columnNameList;
                response.columnTypeList = thcell.columnTypeList;
                response.defaultValue = thcell.defaultValue;
                response.primaryIndex = thcell.primaryIndex;
                response.cellIds = thcell.cellIds;
                response.tableName = thcell.tableName;
            }
        }

        public override void SelectFromProxyHandler(SelectMessageReader request)
        {
            //List<List<long>> cellIds = request.rowIds;
            //List<List<long>> result = new List<List<long>>();  //返回的信息
            //for (int i = 0; i < request.rowIds.Count; i++)
            //{
            //    result.Add(new List<long>());
            //}
            //List<List<int>> types = request.columnTypes;
            ////利用发送来的信息构造condition
            //Table table = new Table(request.rowIds, request.tableNames,
            //    request.tableIds, request.indexes, request.columnTypes, request.columnNames,
            //    request.primaryIndexes, request.defaultValues);
            //Condition con = new Condition(table, request.condition);
            //for (int i = 0; i < cellIds[0].Count; i++)
            //{
            //    if (Global.CloudStorage.GetServerIdByCellId(cellIds[0][i])==Global.MyServerId)  //该行的前几列存储于本服务器上
            //    {
            //        //将数据进行转换
            //        List<List<Object>> values = new List<List<object>>();
            //        values.Add(FieldType.getValues(
            //            Global.LocalStorage.UseRowCell(cellIds[0][i]).values, types[0]));
            //        Console.WriteLine("{0},,,{1}",cellIds.Count, cellIds[0].Count);
            //        for (int j = 1; j < cellIds.Count; j++)  //剩余的列从其他服务器获取
            //        {
            //            GetRowMessageWriter grmw = new GetRowMessageWriter(cellIds[j][i]);
            //            values.Add(FieldType.getValues(
            //                Global.CloudStorage.GetRowToDatabaseServer(
            //                    Global.CloudStorage.GetServerIdByCellId(cellIds[j][i]), grmw).row
            //                , types[j]));
            //        }
            //        if (con.getResult(values))  //筛选
            //        {
            //            for (int j = 0; j < cellIds.Count; j++)
            //            {
            //                result[j].Add(cellIds[j][i]);
            //            }
            //        }
            //    }
            //}
            ////再把信息发回proxy0
            //Global.CloudStorage.SelectFromServerToDatabaseProxy(0,
            //    new SelectResultResponseWriter(Global.MyServerId, result));
        }

        public override void UpdateDatabaseHandler(UpdateDatabaseMessageReader request)
        {
            long cellId = request.databaseId;
            using (var db = Global.LocalStorage.UseDatabaseCell(cellId))
            {
                db.tableIdList = request.tableIdList;
                db.tableNameList = request.tableNameList;
            }
        }

        public override void UpdateElementHandler(UpdateElementMessageReader request)
        {
            using (var eleCell = Global.LocalStorage.UseElementCell(request.eleId))
            {
                eleCell.intField = request.ele.intField;
                eleCell.stringField = request.ele.stringField;
                eleCell.doubleField = request.ele.doubleField;
                eleCell.longField = request.ele.longField;
                eleCell.dateField = request.ele.dateField;

            }
        }

        public override void UpdateTableHandler(UpdateTableMessageReader request)
        {
            using (var thcell = Global.LocalStorage.UseTableHeadCell(request.tableId))
            {
                thcell.tableName = request.tableName;
                thcell.columnNameList = request.columnNameList;
                thcell.columnTypeList = request.columnTypeList;
                thcell.primaryIndex = request.primaryIndex;
                thcell.defaultValue = request.defaultValue;
                thcell.cellIds = request.cellIds;
            }
        }

        public override void DeleteFromProxyHandler(DeleteMessageReader request)
        {
            List<List<long>> cellIds = request.cellIds;
            Table table = new Table(cellIds);
            Condition con = new Condition(table, request.con);
            List<List<long>> result = new List<List<long>>();
            List<Thread> threads = new List<Thread>();
            foreach (List<long> cellId in cellIds)
            {
                if (Global.CloudStorage.GetServerIdByCellId(cellId[0]) == Global.MyServerId)  //这一行第一列存储于本服务器
                {
                    //每行开一个线程处理
                    RowMessage rm = new RowMessage();
                    rm.cellId = cellId;
                    rm.con = con;
                    rm.types = request.columnTypeList;
                    rm.result = result;
                    Thread thread = new Thread(new ParameterizedThreadStart(DeleteThread));
                    threads.Add(thread);
                    thread.Start(rm);
                }
            }
            foreach (Thread thr in threads)
            {
                thr.Join();
            }
            //此时已经处理完结果
            Global.CloudStorage.DeleteFromServerToDatabaseProxy(0,
                new DeleteResultResponceWriter(Global.MyServerId, result));
        }
        private void DeleteThread(Object rowMessage)
        {
            RowMessage rm = (RowMessage)rowMessage;

            List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                Global.CloudStorage.GetServerIdByCellId(rm.cellId[0]),
                new GetRowMessageWriter(rm.cellId)).row;
            List<Object> values = FieldType.getValues(row, rm.types);
            if (rm.con.getResult(values))
            {
                rm.result.Add(rm.cellId);
            }
            foreach (long id in rm.cellId)
            {
                Global.LocalStorage.RemoveCell(id);
            }
        }

        public override void TruncateFromProxyHandler(TruncateMessageReader request)
        {
            List<List<long>> cellIds = request.cellIds;
            List<Thread> threads = new List<Thread>();
            foreach (List<long> cellId in cellIds)
            {
                if (Global.CloudStorage.GetServerIdByCellId(cellId[0]) == Global.MyServerId)  //这一行第一列存储于本服务器
                {
                    RowMessage rm = new RowMessage();
                    rm.cellId = cellId;
                    //每行开一个线程处理
                    Thread thread = new Thread(new ParameterizedThreadStart(TruncateThread));
                    threads.Add(thread);
                    thread.Start(rm);
                }
            }
            foreach (Thread thr in threads)
            {
                thr.Join();
            }
            //此时已经处理完结果
            Global.CloudStorage.TruncateFromServerToDatabaseProxy(0,
                new TruncateResponceWriter(Global.MyServerId));
        }
        private void TruncateThread(Object rowMessage)
        {
            RowMessage rm = (RowMessage)rowMessage;
            foreach (long id in rm.cellId)
            {
                Global.LocalStorage.RemoveCell(id);
            }
        }
        public override void InsertElementHandler(InsertMessageReader request, InsertResponceWriter response)
        {
            List<long> cellIds = new List<long>();
            for (int i = 0; i < request.ele.Count; i++)
            {
                ElementCell elecell = FieldType.getElementCell(request.ele[i]);
                cellIds.Add(elecell.CellID);
                Global.LocalStorage.SaveElementCell(elecell);
            }
            response.serverId = Global.MyServerId;
            response.cellIds = cellIds;
        }
    }
}