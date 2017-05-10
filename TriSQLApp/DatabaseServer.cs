using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using Trinity.Core.Lib;
using System.Threading;
using System.Collections.Concurrent;

namespace TriSQLApp
{
    class DatabaseServer : DatabaseServerBase
    {
        #region 多线程select和delete需要用到的message
        struct RowMessage
        {
            public int threadCount;
            public int threadIndex;
            public List<List<long>> cellIds;
            public List<int> types;
            public List<int> usedIndex;
            public Condition con;
            public ConcurrentBag<List<long>> result;  //保证并发同步
            public RowMessage(int threadCount, int threadIndex, List<List<long>> cellIds, List<int> types,
                List<int> usedIndex, Condition con, ConcurrentBag<List<long>> result)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.cellIds = cellIds;
                this.types = types;
                this.usedIndex = usedIndex;
                this.con = con;
                this.result = result;
            }
        }
        #endregion

        public DatabaseServer():base()
        {
            //Global.LocalStorage.LoadStorage();
        }


        /// <summary>
        /// 查询数据库信息
        /// </summary>
        /// <param name="request"></param>
        /// <param name="response"></param>
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
            using (var ec = Global.LocalStorage.UseElementCell(request.cellId))
            {
                response.ele = FieldType.getElement(ec);
            }
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
                    using (var ec = Global.LocalStorage.UseElementCell(eleId))
                    {
                        response.row.Add(FieldType.getElement(ec));
                    }
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
            List<List<long>> cellIds = request.cellIds;
            //利用发送来的信息构造condition
            Table table = new Table(request.columnNameList, request.columnTypeList);
            Console.WriteLine(request.nestedColumns == null);
            Condition con = new Condition(table, request.condition, request.nestedColumns);
            ConcurrentBag<List<long>> result = new ConcurrentBag<List<long>>();
            //计算线程数
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                RowMessage rm = new RowMessage(threadCount, threadIndex, cellIds, request.columnTypeList,
                    request.usedIndex, con, result);
                threadNum[threadIndex] = new Thread(filter);
                threadNum[threadIndex].Start(rm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            //此时已经处理完结果
            Console.WriteLine("num:" + result.ToList().Count);
            Global.CloudStorage.SelectFromServerToDatabaseProxy(0,
                new SelectResultResponseWriter(Global.MyServerId, result.ToList()));
        }

        /// <summary>
        /// 在线程内对某一行进行判断
        /// </summary>
        /// <param name="cellId"></param>
        private void filter(Object rowMessage)
        {
            RowMessage rm = (RowMessage)rowMessage;
            Condition con = rm.con;
            //计算要判断的行
            int start = -1;
            int end = -1;
            int c = rm.cellIds.Count;
            int ele = c / rm.threadCount;
            if (rm.threadCount != rm.threadIndex + 1)
            {
                start = rm.threadIndex * ele;
                end = start + ele - 1;
            }
            else
            {
                start = rm.threadIndex * ele;
                end = c - 1;
            }
            List<Element> row;
            List<Object> values;
            for (int i = start; i <= end; i++)
            {
                //Condition con = new Condition(rm.table, rm.conStr);
                row = Global.CloudStorage.GetRowToDatabaseServer(
                    Global.CloudStorage.GetServerIdByCellId(rm.cellIds[i][0]),
                    new GetRowMessageWriter(rm.cellIds[i])).row;
                values = FieldType.getValues(row, rm.types);
                if (con.getResult(values))
                {
                    List<long> cellId = new List<long>();
                    foreach (int index in rm.usedIndex)
                    {
                        cellId.Add(rm.cellIds[i][index]);
                    }
                    rm.result.Add(cellId);
                }
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
            ConcurrentBag<List<long>> result = new ConcurrentBag<List<long>>();
            //计算线程数
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                RowMessage rm = new RowMessage(threadCount, threadIndex, cellIds, request.columnTypeList,
                    null, con, result);
                threadNum[threadIndex] = new Thread(DeleteThread);
                threadNum[threadIndex].Start(rm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();

            //此时已经处理完结果
            Global.CloudStorage.DeleteFromServerToDatabaseProxy(0,
                new DeleteResultResponceWriter(Global.MyServerId, result.ToList()));
        }
        private void DeleteThread(Object rowMessage)
        {
            RowMessage rm = (RowMessage)rowMessage;
            Condition con = rm.con;

            //计算要判断的行
            int start = -1;
            int end = -1;
            int c = rm.cellIds.Count;
            int ele = c / rm.threadCount;
            if (rm.threadCount != rm.threadIndex + 1)
            {
                start = rm.threadIndex * ele;
                end = start + ele - 1;
            }
            else
            {
                start = rm.threadIndex * ele;
                end = c - 1;
            }
            List<Element> row;
            List<Object> values;
            for (int i = start; i <= end; i++)
            {
                row = Global.CloudStorage.GetRowToDatabaseServer(
                    Global.CloudStorage.GetServerIdByCellId(rm.cellIds[i][0]),
                    new GetRowMessageWriter(rm.cellIds[i])).row;
                values = FieldType.getValues(row, rm.types);
                if (con.getResult(values))
                {
                    rm.result.Add(rm.cellIds[i]);
                    foreach (long id in rm.cellIds[i])
                    {
                        Global.CloudStorage.RemoveCell(id);
                    }
                }
            }
        }
        public override void InsertElementHandler(InsertMessageReader request, InsertResponceWriter response)
        {
            List<long> cellIds = new List<long>();
            for (int i = 0; i < request.ele.Count; i++)
            {
                ElementCell elecell = FieldType.getElementCell(request.ele[i]);
                cellIds.Add(elecell.CellID);
                Global.CloudStorage.SaveElementCell(elecell);
            }
            response.serverId = Global.MyServerId;
            response.cellIds = cellIds;
        }

        public override void SaveStorageHandler()
        {
            Global.LocalStorage.SaveStorage();
        }

        public override void DoJoinFromProxyHandler(JoinMessageReader request)
        {
            Table Ta = new Table(request.cellidsA);
            Table Tb = new Table(request.cellidsB);
            List<dint> cond = new List<dint>();
            for (int i = 0; i < request.conda.Count; i++)
            {
                cond.Add(new dint(request.conda[i], request.condb[i]));
            }
            JoinResponceWriter msg = new JoinResponceWriter(Global.MyServerId, (Ta.innerJoin(Tb, null ,cond, false)).getCellIds());
            Global.CloudStorage.REdoJoinFromServerToDatabaseProxy(0, msg);
        }

        public override void TopKFromProxyHandler(TopKMessageReader request)
        {
            List<List<long>> cellids = request.celllids;
            List<List<Element>> correspon = Table.getCorrespon(request.cond, cellids);
            int k = request.k;
            List<List<long>> res = new List<List<long>>(k);
            List<int> cmp = new List<int>(k);
            for (int i = 0; i < k; i++)
            {
                cmp.Add(-1);
                res.Add(null);
            }
            for (int a = 0; a < correspon.Count; a++)
            {
                bool flag = false;
                int p = 0;
                while (p < k && correspon[a][0].intField > cmp[p])
                {
                    ++p;
                    flag = true;
                }
                if (flag)
                {
                    int tint = correspon[a][0].intField;
                    List<long> tres = cellids[a];
                    while (p - 1 >= 0)
                    {
                        int tint2 = cmp[p - 1];
                        List<long> tres2 = res[p - 1];
                        cmp[p - 1] = tint;
                        res[p - 1] = tres;
                        tint = tint2;
                        tres = tres2;
                        --p;
                    }
                }
            }
            for (int i = 0; i < (k) / 2; i++)
            {
                int tint = cmp[i];
                List<long> tres = res[i];
                cmp[i] = cmp[k - i - 1];
                res[i] = res[k - i - 1];
                cmp[k - i - 1] = tint;
                res[k - i - 1] = tres;
            }
            TopKServerResponceWriter msg = new TopKServerResponceWriter(res, cmp, Global.MyServerId);
            Global.CloudStorage.TopKFromServerToDatabaseProxy(0, msg);
        }

        public override void UnionFromProxyHandler(UnionMessageReader request)
        {
            
            if (request.cellidsA.Count == 0)
            {
                UnionServerResponseWriter msg = new UnionServerResponseWriter(request.cellidsB, Global.MyServerId);
                Global.CloudStorage.UnionFromServerToDatabaseProxy(0, msg);
            } else
            {
                List<List<long>> res = new List<List<long>>();
                List<int> conda = new List<int>();
                List<List<long>> cellids = request.cellidsA;
                List<List<long>> cellidsb = request.cellidsB;

                for (int i = 0; i < cellids[0].Count; i++) 
                {
                    conda.Add(i);
                }
                List<List<Element>> correspondA = Table.getCorrespon(conda, cellids);
                List<List<Element>> correspondB = Table.getCorrespon(conda, cellidsb);
                Table.QuickSort(correspondA, 0, correspondA.Count - 1, cellids);
                for (int i = 0; i<correspondB.Count; i++)
                {
                    if (Table.BinSearch(correspondA,correspondB[i],0) < 0)//don't match
                    {
                        res.Add(cellidsb[i]);
                    }
                }
                UnionServerResponseWriter msg = new UnionServerResponseWriter(res, Global.MyServerId);
                Global.CloudStorage.UnionFromServerToDatabaseProxy(0, msg);
            }
        }
    }
}
