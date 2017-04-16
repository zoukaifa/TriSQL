using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;

namespace TriSQLApp
{
    //以下全是测试代码
    internal class Program
    {

        static void Main(string[] args)
        {
            TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5305, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5305, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5306, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5307, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            DatabaseServer ds = new DatabaseServer();
            ds.Start();
            
        }
    }
}