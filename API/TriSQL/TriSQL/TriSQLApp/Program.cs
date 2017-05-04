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
            if (args.Length > 0 && args[0].Equals("-k"))
            {
                Database ssss = new Database("test");
                Table graph = new Table("Graph");
                Table label = new Table("Label");
                graph.printTable();
                label.printTable();
                List<dint> con = new List<dint> { new dint(1, 0) };
                Table ne = graph.innerJoin(label, con);
                ne.printTable();
                Console.ReadKey();
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
            else
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                //Global.CloudStorage.ResetStorage();

                Database.createDatabase("test");
                //    Database.createDatabase("test2");
                //    Database.createDatabase("test3");
                //    Database.createDatabase("test4");
                Database database = new Database("test");

                Table graph = database.createTable("Graph", new string[] { },
                    new Tuple<int, string, object>(FieldType.INTEGER, "node1", 0),
                    new Tuple<int, string, object>(FieldType.INTEGER, "node2", 0));
                Table label = database.createTable("Label", new string[] { },
                    new Tuple<int, string, object>(FieldType.INTEGER, "node", 0),
                    new Tuple<int, string, object>(FieldType.INTEGER, "label", 0));
                Table temp = database.createTable("Temp", new string[] { },
                    new Tuple<int, string, object>(FieldType.INTEGER, "label", 0),
                    new Tuple<int, string, object>(FieldType.INTEGER, "num", 0),
                    new Tuple<int, string, object>(FieldType.DOUBLE, "ran", 0));

                graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 1 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 2 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 3 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 2 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 3 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 4 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 5 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 4 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 11 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 6 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 7 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 7 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 8 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 11 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 11 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 12 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 13 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 12 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 13 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 13 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 0 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 2, 0 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 0 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 2, 1 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 1 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 4, 1 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 1 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 4, 3 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 3 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 3 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 5 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 5 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 5 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 6 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 6 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 7 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 8 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 10 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 10 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 10 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 11 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 11 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 12 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 8 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 5 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 9 });
                graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 6 });
                int nodeNum = 14;
                int iteNum = 1;
                for (int i = nodeNum; i > 0; i--)
                {
                    label.insert(new string[] { "node", "label" }, new object[] { nodeNum - i, nodeNum - i });
                    temp.insert(new string[] { "label", "num", "ran" }, new object[] { nodeNum - i, 0, new Random().NextDouble() });
                }
            }
        }
        static void Main1(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("-k"))
            {
                //Global.CloudStorage.LoadStorage();
                Database database = new Database("test");
                Table tableA = new Table("A");
                Table tableB = new Table("B");
                Table tableAA = new Table("AA");
                List<dint> con = new List<dint>();
                con.Add(new dint(0, 0));
                con.Add(new dint(1, 1));
                Console.WriteLine(tableA.getTableNames()[0]);
                tableA.printTable();

                Console.WriteLine(tableB.getTableNames()[0]);
                tableB.printTable();

                Table newtable;
                //本地调用
                /*
                //条件join
                newtable = tableA.innerJoin(tableB,con);
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                //默认条件join
                newtable = tableA.innerJoin(tableB);
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                //笛卡尔积
                newtable = tableA.innerJoin(tableB, new List<dint>());
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                */
                //全局调用
                /*
                //条件join
                newtable = tableA.innerJoinOnCluster(tableB, con);
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                //默认条件join
                newtable = tableA.innerJoinOnCluster(tableB);
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                //笛卡尔积
                newtable = tableA.innerJoinOnCluster(tableB, new List<dint>());
                Console.WriteLine(newtable.getTableNames()[0]);
                newtable.printTable();
                */

                /*
                //topk on local
                newtable = new Table(tableA.topKOnLocal(1, "b"), tableA.getColumnTypes());
                newtable.printTable();
                //topk on cluster
                newtable = new Table(tableA.topK(1, new string[] { "b" }), tableA.getColumnTypes());
                newtable.printTable();
                */

                /*
                //union
                tableA.printTable();
                tableAA.printTable();
                tableAA.union_distinct(tableA);
                tableAA.printTable();
                */

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
            else
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                //Global.CloudStorage.ResetStorage();

                Database.createDatabase("test");
                //    Database.createDatabase("test2");
                //    Database.createDatabase("test3");
                //    Database.createDatabase("test4");
                Database database = new Database("test");

                Table tableA = database.createTable("A", null,
                                new Tuple<int, string, object>(FieldType.INTEGER, "a", 0),
                                new Tuple<int, string, object>(FieldType.INTEGER, "b", 0),
                                new Tuple<int, string, object>(FieldType.INTEGER, "c", 0));

                Table tableAA = database.createTable("AA", null,
                                new Tuple<int, string, object>(FieldType.INTEGER, "a", 0),
                                new Tuple<int, string, object>(FieldType.INTEGER, "b", 0),
                                new Tuple<int, string, object>(FieldType.INTEGER, "c", 0));

                Table tableB = database.createTable("B", null,
                                new Tuple<int, string, object>(FieldType.INTEGER, "a", 0),
                                new Tuple<int, string, object>(FieldType.INTEGER, "b", 0));

                Table tableC = database.createTable("C", null,
                    new Tuple<int, string, object>(FieldType.INTEGER, "b", 0),
                    new Tuple<int, string, object>(FieldType.INTEGER, "c", 0));

                //Table tableA = new Table("tableA");
                //Table tableB = new Table("tableB");
                //Table tableC = new Table("tableC");

                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 30 });
                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 21, 30 });
                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 31 });
                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 11, 20, 30 });
                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 22, 31 });
                tableA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 22, 36 });

                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 30 });
                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 21, 30 });
                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 20, 31 });
                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 11, 20, 30 });
                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 10, 22, 31 });
                tableAA.insert(new string[] { "a", "b", "c" }, new Object[] { 15, 22, 31 });

                tableB.insert(new string[] { "a", "b" }, new Object[] { 10, 20 });
                tableB.insert(new string[] { "a", "b" }, new Object[] { 10, 21 });
                tableB.insert(new string[] { "a", "b" }, new Object[] { 11, 20 });
                tableB.insert(new string[] { "a", "b" }, new Object[] { 11, 21 });

                tableC.insert(new string[] { "b", "c" }, new Object[] { 20, 30 });
                tableC.insert(new string[] { "b", "c" }, new Object[] { 21, 30 });
                tableC.insert(new string[] { "b", "c" }, new Object[] { 20, 31 });

                tableA.printTable();
                Global.CloudStorage.SaveStorage();
                Console.ReadKey();
            }
        }
    }
}