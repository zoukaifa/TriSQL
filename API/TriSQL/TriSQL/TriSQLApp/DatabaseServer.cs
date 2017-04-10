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

        public override void GetTableHandler(GetTableMessageReader request, GetTableResponseWriter response)
        {
            throw new NotImplementedException();
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
