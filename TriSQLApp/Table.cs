using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Trinity;
using TriSQL;
using Trinity.Core.Lib;

namespace TriSQLApp
{
    class Table
    {
        private string tableName;
        private List<List<long>> cellIds;
        private List<string> tableNames;
        private List<long> tableIds;
        private List<List<int>> indexes;
        private List<List<string>> columnNames;

        //打印试试
        public void printTable()
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                Console.WriteLine(tableNames[i] + ":");
                foreach (var a in columnNames[i])
                {
                    Console.Write(a + '\t');
                }
                Console.WriteLine();
                foreach (var a in cellIds[i])
                {
                    foreach (var b in Global.CloudStorage.LoadRowCell(a).values)
                    {
                        Console.Write(b.stringField + '\t');
                    }
                    Console.WriteLine();
                }
            }
        }
        //不做笛卡尔
        public Table(String databaseName, params string[] tablename)
        {
            if (tablename.Length != 0)
            {
                Database database = new Database(databaseName);
                List<long> l = new List<long>();
                foreach (var table in tablename)
                {
                    var tableId = database.getTableIdList().ElementAt(database.tableIndex(table));
                    l.Add(tableId);
                }
                Global.CloudStorage.GetTableToGetTableProxy(0, new GetTableMessageWriter(l));
                TableResponse g = new TableResponse();
                g.tables = new List<GetTableResponse>();
                while (true)
                {
                    Thread.Sleep(100);
                    foreach (var t in Global.CloudStorage.ResponseTablesToGetTableProxy(0).tables)
                    {
                        g.tables.Add(t);
                    }
                    if (g.tables.Count == 0) continue;
                    else break;
                }
                tableNames = new List<string>();
                columnNames = new List<List<string>>();
                cellIds = new List<List<long>>();
                foreach (var t in g.tables)
                {
                    tableNames.Add(t.tableName);
                    columnNames.Add(t.columnNameList);
                    List<long> row = t.rowList;
                    cellIds.Add(row);
                }
            }
        }

        public void delete(Condition con=null)
        {

        }

        public void update(string[] fieldName, object[] values, Condition con=null)
        {

        }

        public void update(string[] fieldName, Table anotherTable, Condition con=null)
        {

        }

        public void insert(string[] fieldNames, object[] values)
        {

        }

        public void insert(string[] fieldNames, object[][] values)
        {

        }

        public void insert(string[] fieldNames, Table anotherTable)
        {

        }

        public void truncate()
        {

        }

        public void rename(string newName)
        {

        }

        public Table union(Table anotherTable)
        {
            return null;
        }

        public Table innerJoin(Table anotherTable)
        {
            return null;
        }

        public Table select(Tuple<Condition, int, string>[] fields, Table[] table, Condition con, ref object[] vars)
        {
            return null;
        }
    }
}
