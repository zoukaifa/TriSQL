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
        static void Main1(string[] args)
        {
            
            //List<int> a = new List<int> { 43, 90, 80, 15, 789, 27, 90, 69, 90, 158, 45, 32, 90, 22, 77, 66, 90 };
            List<int> a = new List<int> { 13, 12, 11, 19, 10 };
            List<int> p = new List<int> { 20, 21, 20, 20, 22 };
            List<List<Element>> b = new List<List<Element>>();
            int c = 0;
            Table T = new Table();
            for (int aa = 0; aa<a.Count; aa++)
            {
                Element e = new Element(intField: a[aa]);
                Element ep = new Element(intField: p[aa]);
                List<Element> ee = new List<Element>();
                ee.Add(e);
                ee.Add(ep);
                b.Add(ee);
                List<long> l = new List<long>();
                l.Add(c++);
                T.getCellIds().Add(l);
            }
            foreach(var pppp in b)
            {
                Console.WriteLine(b[0][0].intField.ToString() + " " + b[0][1].intField.ToString() +" "+ Table.CopTo(b[0], pppp) +" "+ pppp[0].intField+" "+ pppp[1].intField);
            }
            foreach (var oo in b)
            {
                foreach (var pp in oo)
                {
                    Console.Write(pp.intField+" ");
                }
                Console.WriteLine();
            }

            Console.WriteLine("------------");
            Table.QuickSort(b, 0, b.Count - 1, T);
            foreach(var oo in b)
            {
                foreach(var pp in oo)
                {
                    Console.Write(pp.intField+" ");
                }
                Console.WriteLine();
            }
            
            Console.ReadLine();
        }

        static void Main(string[] args)
        {
            if (args.Length > 0 && args[0].Equals("-k"))
            {
                //Global.CloudStorage.LoadStorage();
                Database database = new Database("test");
                Table tableA = new Table("A");
                Table tableB = new Table("B");
                List<dint> con = new List<dint>();
                con.Add(new dint(0, 0));
                con.Add(new dint(1, 1));
                Console.WriteLine(tableA.tableNames[0]);
                tableA.printTable();

                Console.WriteLine(tableB.tableNames[0]);
                tableB.printTable();

                Table newtable;
                //本地调用
                /*
                //条件join
                newtable = tableA.innerJoin(tableB,con);
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
                //默认条件join
                newtable = tableA.innerJoin(tableB);
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
                //笛卡尔积
                newtable = tableA.innerJoin(tableB, new List<dint>());
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
                */
                //全局调用
                /*
                //条件join
                newtable = tableA.innerJoinOnCluster(tableB, con);
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
                //默认条件join
                newtable = tableA.innerJoinOnCluster(tableB);
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
                //笛卡尔积
                newtable = tableA.innerJoinOnCluster(tableB, new List<dint>());
                Console.WriteLine(newtable.tableNames[0]);
                newtable.printTable();
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