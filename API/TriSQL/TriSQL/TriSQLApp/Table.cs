using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TriSQLApp
{
    class Table
    {
        private string databaseName;
        private List<List<long>> cellIds;
        private List<string> tableNames;
        private List<long> tableIds;
        private List<List<int>> indexes;
        private List<List<string>> columnNames;

        public Table(params string[] tableNames)
        {

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
