using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;

namespace TriSQLApp
{
    class DatabaseServer : DatabaseServerBase
    {
        public override void CreateTableHandler(CreateTableMessageReader request)
        {
            throw new NotImplementedException();
        }

        public override void GetDatabaseHandler(GetDatabaseMessageReader request, GetDatabaseResponseWriter response)
        {
            throw new NotImplementedException();
        }

        public override void GetTableHandler(GetTableMessageReader request, GetTableResponseWriter response)
        {
            throw new NotImplementedException();
        }
    }
}
