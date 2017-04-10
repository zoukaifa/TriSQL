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
        public override void CreateTableHandler(CreateTableMessageReader request)
        {
            
        }


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
                    response.tableList = db.tableList;
                    response.tableNameList = db.tableNameList;
                }
            }
            
        }

        public override void GetTableHandler(GetTableMessageReader request, GetTableResponseWriter response)
        {
            throw new NotImplementedException();
        }
    }
}
