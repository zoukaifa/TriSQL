using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using Trinity.Core.Lib;

namespace TriSQLApp
{
    class DatabaseServer : DatabaseServerBase
    {
        /// <summary>
        /// 查询数据库信息
        /// </summary>
        /// <param name="request"></param>
        /// <param name="response"></param>
        public override void GetDatabaseHandler(GetDatabaseMessageReader request, GetDatabaseResponseWriter response)
        {
            long cellId = HashHelper.HashString2Int64(request.name);
            using (var db = Global.LocalStorage.UseDatabaseCell(cellId)) {
                response.tableIdList = db.tableIdList;
                response.tableNameList = db.tableNameList;
            }
            
        }

        public override void GetRowHandler(GetRowMessageReader request, GetRowResponseWriter response)
        {
            Console.WriteLine("GetRow:{0}", request.cellId);
            Console.WriteLine(Global.LocalStorage.UseRowCell(request.cellId));
            Console.WriteLine("1111");
            using (var rowCell = Global.LocalStorage.UseRowCell(request.cellId))
            {
                response.row = rowCell.values;
            }
        }

        public override void GetTableHandler(GetTableMessageReader request, GetTableResponseWriter response)
        {
            using (var thcell = Global.LocalStorage.UseTableHeadCell(request.tableId))
            {
                response.tableName = thcell.tableName;
                response.columnNameList = thcell.columnNameList;
                response.columnTypeList = thcell.columnTypeList;
                response.primaryIndex = thcell.primaryIndex;
                response.defaultValue = thcell.defaultValue;
                response.rowList = thcell.rowList;
            }
        }

        public override void SelectFromProxyHandler(SelectMessageReader request)
        {
            List<List<long>> cellIds = request.rowIds;
            List<List<long>> result = new List<List<long>>();  //返回的信息
            for (int i = 0; i < request.rowIds.Count; i++)
            {
                result.Add(new List<long>());
            }
            List<List<int>> types = request.columnTypes;
            //利用发送来的信息构造condition
            Table table = new Table(request.rowIds, request.tableNames,
                request.tableIds, request.indexes, request.columnTypes, request.columnNames,
                request.primaryIndexes, request.defaultValues);
            Condition con = new Condition(table, request.condition);
            for (int i = 0; i < cellIds[0].Count; i++)
            {
                if (Global.CloudStorage.GetServerIdByCellId(cellIds[0][i])==Global.MyServerId)  //该行的前几列存储于本服务器上
                {
                    //将数据进行转换
                    List<List<Object>> values = new List<List<object>>();
                    values.Add(FieldType.getValues(
                        Global.LocalStorage.UseRowCell(cellIds[0][i]).values, types[0]));
                    Console.WriteLine("{0},,,{1}",cellIds.Count, cellIds[0].Count);
                    for (int j = 1; j < cellIds.Count; j++)  //剩余的列从其他服务器获取
                    {
                        GetRowMessageWriter grmw = new GetRowMessageWriter(cellIds[j][i]);
                        values.Add(FieldType.getValues(
                            Global.CloudStorage.GetRowToDatabaseServer(
                                Global.CloudStorage.GetServerIdByCellId(cellIds[j][i]), grmw).row
                            , types[j]));
                    }
                    if (con.getResult(values))  //筛选
                    {
                        for (int j = 0; j < cellIds.Count; j++)
                        {
                            result[j].Add(cellIds[j][i]);
                        }
                    }
                }
            }
            //再把信息发回proxy0
            Global.CloudStorage.SelectFromServerToDatabaseProxy(0,
                new SelectResultResponseWriter(Global.MyServerId, result));
        }

        public override void UpdateDatabaseHandler(UpdateDatabaseMessageReader request)
        {
            long cellId = HashHelper.HashString2Int64(request.name);
            using (var db = Global.LocalStorage.UseDatabaseCell(cellId))
            {
                db.tableIdList = request.tableIdList;
                db.tableNameList = request.tableNameList;
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
                thcell.rowList = request.rowList;
            }
        }
    }
}
