using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TriSQLApp
{
    class Database {
        private string name;
        private List<long> tableIdList;
        private List<string> tableNameList;

        public Database(string name) {
        }

        public static Database createDatabase(string name) {
            return null;
        }

        public Table createTable(string name, string[] primaryKeyList, params Tuple<int, string, object>[] fields)
        {
            return null;
        }

        public bool exists(string name)
        {
            return true;
        }

        public void drop(string name)
        {

        }
    }
}
