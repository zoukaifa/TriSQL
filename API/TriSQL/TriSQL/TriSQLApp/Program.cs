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
            TrinityConfig.AddServer(new Trinity.Network.ServerInfo("127.0.0.1", 5304, Global.MyAssemblyPath, Trinity.Diagnostics.LogLevel.Error));
            //DatabaseServer ds = new DatabaseServer();
            //ds.Start();
            TrinityConfig.CurrentRunningMode = RunningMode.Client;
            Global.CloudStorage.LoadStorage();
            //Database.createDatabase("test");
            Database database = new Database("test");
            //Table tableA = database.createTable("tableA", new string[] {"class"}, 
            //new Tuple<int, string, object>(FieldType.INTEGER, "class", 2),
            //new Tuple<int, string, object>(FieldType.STRING, "name", "hhh"));
            //Table tableB = database.createTable("tableB", new string[] {"class"}, 
            //new Tuple<int, string, object>(FieldType.INTEGER, "class", 2),
            //new Tuple<int, string, object>(FieldType.STRING, "name", "hhh"));
            //database.dropTable("tableB");
            for (int i = 0; i < database.getTableNameList().Count; i++)
            {
                Console.WriteLine("{0}, {1}", database.getTableNameList().ElementAt(i),
                    database.getTableIdList().ElementAt(i));
            }
        }
    }
}