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
        static void Main2(string[] args)
        {
            List<int> a = new List<int> { 1, 2, 3, 4, 6 };
            foreach (int aa in a)
            {
                Console.WriteLine(aa);
            }
        }

        static void Main(string[] args)
        {
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("192.168.1.112", 5304, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("192.168.1.102", 5304, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5304, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5306, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5307, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            if (args.Length > 0 && args[0].Equals("-s"))
            //{
            {
                //TrinityConfig.CurrentRunningMode = RunningMode.Server;
                DatabaseServer ds = new DatabaseServer();
                ds.Start();
            }
            else if (args.Length > 0 && args[0].Equals("-p"))
            {
                //TrinityConfig.CurrentRunningMode = RunningMode.Proxy;
                DatabaseProxy dp = new DatabaseProxy();
                dp.Start();
            }
            //}
            else
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                //Global.CloudStorage.ResetStorage();
                //Global.CloudStorage.LoadStorage();

                //Database.createDatabase("test");
                //    Database.createDatabase("test2");
                //    Database.createDatabase("test3");
                //    Database.createDatabase("test4");
                Database database = new Database("test");

                //Table tableA = database.createTable("A", null,
                //                new Tuple<int, string, object>(FieldType.INTEGER, "a", 0),
                //                new Tuple<int, string, object>(FieldType.INTEGER, "b", 0),
                //                new Tuple<int, string, object>(FieldType.INTEGER, "c", 0));

                //Table tableB = database.createTable("B", null,
                //                new Tuple<int, string, object>(FieldType.INTEGER, "a", 0),
                //                new Tuple<int, string, object>(FieldType.INTEGER, "b", 0));

                //Table tableC = database.createTable("C", null,
                //    new Tuple<int, string, object>(FieldType.INTEGER, "b", 0),
                //    new Tuple<int, string, object>(FieldType.INTEGER, "c", 0));

                Table tableA = new Table("A");
                Table tableB = new Table("B");
                Table tableC = new Table("C");

                //tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 30 });
                //tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 21, 30 });
                //tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 31 });
                //tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 11, 20, 30 });
                //tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 22, 31 });

                //tableB.insert(new string[] { "a", "b" }, new Object[] { 10, 20 });
                //tableB.insert(new string[] { "a", "b" }, new Object[] { 10, 21 });
                //tableB.insert(new string[] { "a", "b" }, new Object[] { 11, 20 });
                //tableB.insert(new string[] { "a", "b" }, new Object[] { 11, 21 });

                //tableC.insert(new string[] { "b", "c" }, new Object[] { 20, 30 });
                //tableC.insert(new string[] { "b", "c" }, new Object[] { 21, 30 });
                //tableC.insert(new string[] { "b", "c" }, new Object[] { 20, 31 });

                List<dint> con = new List<dint>();
                con.Add(new dint(0, 0));
                con.Add(new dint(1, 1));
                //tableA.innerJoin(tableB, con);
                //tableA.printTable();
                //tableB.printTable();
                //tableC.printTable();
                //tableA.update("a", 1, '+', 100, null);
                Table sa = tableA.select(new Tuple<string, string>[]
                {
                    new Tuple<string, string>("a", null),
                    new Tuple<string, string>("c", "qe")
                }, null);
                sa.printTable();
                Table ssa = sa.select(new Tuple<string, string>[] {
                    new Tuple<string, string>("qe", null) }, null);
                ssa.printTable();
                Global.CloudStorage.SaveStorage();
            }

        }
    }
}