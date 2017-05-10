using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using Trinity.Core.Lib;
using TriSQL;

namespace TriSQLApp
{
    //以下全是测试代码
    internal class Program
    {
        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("-k"))  //导入数据
            {
                //Global.CloudStorage.LoadStorage();
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                SubgraphMatch lpa = new SubgraphMatch();
                lpa.init();
                lpa.create();
                lpa.usedatabase();
                Console.ReadLine();
            }
            else
                if (args.Length > 0 && args[0].Equals("-s"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Server;
                DatabaseServer ds = new DatabaseServer();
                ds.Start();
            }
            else if (args.Length > 0 && args[0].Equals("-p"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Proxy;
                DatabaseProxy dp = new DatabaseProxy();
                dp.Start();
            }
            else if (args.Length > 0 && args[0].Equals("-t"))
            {
                SubgraphMatch lpa = new SubgraphMatch();
                lpa.usedatabase();
                lpa.query();
            }
            else
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                //Global.CloudStorage.ResetStorage();
                LPA lpa = new LPA();
                lpa.initTable();
                lpa.cal();
                //if (Database.exists("TIANCHAO"))
                //{
                //    Database.dropDatabase("TIANCHAO");
                //}
                //RegularMatch rm = new RegularMatch("a*bd");
                Console.ReadKey();
            }
        }
    }
}