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
            response.exists = false;
            if (Global.CloudStorage.IsLocalCell(cellId)) {  //在本服务器上
                response.exists = true;
                using (var db = Global.LocalStorage.UseDatabaseCell(cellId)) {
                    response.tableIdList = db.tableIdList;
                    response.tableNameList = db.tableNameList;
                }
            }
            
        }
        /// <summary>
        /// proxy 调用 protocal RequireTable to server
        /// 发送一个 cellid
        /// server 调用 protocol ResponseTable to proxy
        /// 发送一个 cell boob
        /// 将本地的cellid的cell传回proxy,这里可以使用loadcell 来代替,如果把ResponseTable cell改为general cell 可以返回任意cell
        /// </summary>
        /// <param name="request"></param>
        public override void RequireTableHandler(CellIdMessageReader request)
        {
            using (var accessor =  Global.LocalStorage.UseTableHeadCell(request.cellid)){
                Global.CloudStorage.ResponseTableToGetTableProxy(0, new GetTableResponseWriter(accessor.tableName, accessor.columnNameList,
                    accessor.columnTypeList, accessor.primaryIndex, accessor.defaultValue, accessor.rowList));
            }
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
    }
}
