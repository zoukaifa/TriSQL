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
    internal class GetTableProxy : GetTableProxyBase
    {
        private List<GetTableResponse> result = new List<GetTableResponse>();
        private int count = 0;
        /// <summary>
        /// client 调用 protocol GetTable to proxy
        /// 发送 几个 cellids
        /// proxy 调用 protocol RequireTable to server
        /// 发送 一个 cellid
        /// </summary>
        /// <param name="request"></param>
        public override void GetTableHandler(GetTableMessageReader request)
        {
            List<long> cellIds = request.tableIds;
            count = cellIds.Count;
            foreach (var cellId in cellIds)
            {
                var severid = Global.CloudStorage.GetServerIdByCellId(cellId);
                Global.CloudStorage.RequireTableToDatabaseServer(severid, new CellIdMessageWriter(cellId));
            }
        }
        /// <summary>
        /// sever 调用 protocol ResponseTable to proxy
        /// 发送 一个 表信息
        /// proxy 接收到 result
        /// </summary>
        /// <param name="request"></param>
        public override void ResponseTableHandler(GetTableResponseReader request)
        {
            result.Add(request); ;
            if (result.Count == count)
            {
                foreach (var a in result)
                    Console.WriteLine(a.tableName);
            }
        }
        /// <summary>
        /// client 调用 protocol ResponseTables to proxy
        /// proxy 返回 result
        /// </summary>
        /// <param name="response"></param>
        public override void ResponseTablesHandler(TableResponseWriter response)
        {
            if (result.Count == count)
            {
                response.tables = result;
                count = 0;
                result.Clear();
            }
        }
    }
}
