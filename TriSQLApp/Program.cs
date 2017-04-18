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
            //TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5304, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));

            if (args.Length < 1)
            {
                Console.WriteLine("Please provide a command line parameter (-s|-c).");
                Console.WriteLine("  -s  Start as a server.");
                Console.WriteLine("  -c  Start as a client.");
                return;
            }

            if (args[0].Trim().ToLower().StartsWith("-s"))//server
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Server;
                DatabaseServer ds = new DatabaseServer();
                ds.Start();
            }
            if (args[0].Trim().ToLower().StartsWith("-p"))//proxy
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Proxy;
                GetTableProxy dp = new GetTableProxy();
                dp.Start();
            }
            if (args[0].Trim().ToLower().StartsWith("-q"))//query先运行server proxy client
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                string[] s = new string[] { "tableB", "tableD", "tableF","tableG" };
                Table T = new Table("test", s);
                T.printTable();
            }
            if (args[0].Trim().ToLower().StartsWith("-c"))//client
            {
                Console.WriteLine("try");
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                Database database = Database.createDatabase("test");

                Console.WriteLine("database finished");
                //没有插入数据 "aaa"为默认值
                Table tableA = database.createTable("tableA", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 2),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "aaa"));
                Table tableB = database.createTable("tableB", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));
                Table tableC = database.createTable("tableC", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));
                Table tableD = database.createTable("tableD", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));
                Table tableE = database.createTable("tableE", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));
                Table tableF = database.createTable("tableF", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));
                Table tableG = database.createTable("tableG", new string[] { "class" },
                    new Tuple<int, string, object>(FieldType.INTEGER, "class", 3),
                    new Tuple<int, string, object>(FieldType.STRING, "name", "bbb"));

                Console.WriteLine("table finished");

                for (int i = 0; i < database.getTableNameList().Count; i++)
                {
                    Console.WriteLine("{0}, {1}", database.getTableNameList().ElementAt(i),
                        database.getTableIdList().ElementAt(i));
                }
                //database.dropTable("tableB");
            }

        }
    }
}