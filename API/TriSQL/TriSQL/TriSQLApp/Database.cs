using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using Trinity.Core.Lib;

namespace TriSQLApp
{
    class Database {
        private static Database currentDatabase;  //指示当前的数据库
        private string name = null;
        private List<long> tableIdList = null;
        private List<string> tableNameList = null;

        /// <summary>
        /// 根据已有的数据库名，实例化数据库对象
        /// </summary>
        /// <param name="name">数据库名，必须已存在</param>
        public Database(string name) {
            //如果该数据库不存在，抛出异常
            if (! Global.CloudStorage.Contains(HashHelper.HashString2Int64(name)))
            {
                throw new Exception(String.Format("数据库{0}不存在!", name));
            }

            GetDatabaseMessageWriter messageWriter = new GetDatabaseMessageWriter(name);
            for (int i = 0; i < Global.CloudStorage.ServerCount; i++) {
                using (var resp = Global.CloudStorage.GetDatabaseToDatabaseServer(i, messageWriter))
                {
                    if (resp.exists)  //找到了该数据库
                    {
                        this.name = name;
                        this.tableIdList = resp.tableIdList;
                        this.tableNameList = resp.tableNameList;
                        break;
                    }
                }
            }
            currentDatabase = this;
        }
        /// <summary>
        /// 仅用于createDatabase的构造函数
        /// </summary>
        /// <param name="name">数据库名</param>
        /// <param name="tableIdList">表的cellId列表</param>
        /// <param name="tableNameList">表的名字列表</param>
        private Database(string name, List<long> tableIdList, List<string> tableNameList) {
            this.name = name;
            this.tableIdList = tableIdList;
            this.tableNameList = tableNameList;
           
        }

        /// <summary>
        /// 建立一个新的数据库
        /// </summary>
        /// <param name="name">数据库的名字</param>
        /// <returns>对应于该数据库的Database的实例化对象</returns>
        public static Database createDatabase(string name) {
            //先确认该数据库并不存在
            if (Global.CloudStorage.Contains(HashHelper.HashString2Int64(name))) {
                throw new Exception(String.Format("数据库{0}已经存在!", name));
            }

            DatabaseCell dbc = new DatabaseCell(name: name, tableIdList: new List<long>(),
                tableNameList: new List<string>());
            //以数据库名的hash为cellId，存入云端
            Global.CloudStorage.SaveDatabaseCell(HashHelper.HashString2Int64(name), dbc);
            Global.CloudStorage.SaveStorage();
            Database database = new Database(name, dbc.tableIdList, dbc.tableNameList);
            currentDatabase = database;
            return database;
        }

        public Table createTable(string name, string[] primaryKeyList, params Tuple<int, string, object>[] fields)
        {
            if (tableExists(name))  //表已存在
            {
                throw new Exception(String.Format("表{0}已经存在!", name));
            }
            TableHeadCell thc = new TableHeadCell(tableName: name, columnNameList: new List<string>(),
                columnTypeList: new List<int>(), rowList: new List<long>(), defaultValue: new List<Element>(),
                primaryIndex: new List<int>());
            if (fields == null || fields.Length == 0)
            {
                throw new Exception("建立表至少要有一个字段");
            }
            for (int i = 0; i < fields.Length; i++)
            {
                int type = fields[i].Item1;
                string fieldName = fields[i].Item2;
                object defaultValue = fields[i].Item3;
                if (thc.columnNameList.Contains(fieldName))
                {
                    throw new Exception(String.Format("重复声明的字段:{0}.", fieldName));
                }
                thc.columnNameList.Add(fieldName);
                thc.columnTypeList.Add(type);
                thc.defaultValue.Add(FieldType.setValue(defaultValue, type));
            }
            if (primaryKeyList != null && primaryKeyList.Length > 0)
            {
                for (int i = 0; i < primaryKeyList.Length; i++)
                {
                    string key = primaryKeyList[i];
                    if (! thc.columnNameList.Contains(key))
                    {
                        throw new Exception(String.Format("字段{0}不在字段列表中，不能作为主键.", key));
                    }
                    thc.primaryIndex.Add(thc.columnNameList.IndexOf(key));
                }
            }
            //cell已经建好，存储于云端
            Global.CloudStorage.SaveTableHeadCell(thc);
            //向数据库写入表的信息，并更新到云
            tableIdList.Add(thc.CellID);
            tableNameList.Add(thc.tableName);
            //将cell的数据发送到服务器
            UpdateDatabaseMessageWriter udmw = new UpdateDatabaseMessageWriter(
                name:this.name, tableNameList:this.tableNameList, tableIdList:this.tableIdList);
            int serverId = Global.CloudStorage.GetServerIdByCellId(HashHelper.HashString2Int64(this.name));
            Global.CloudStorage.UpdateDatabaseToDatabaseServer(serverId, udmw);
            Global.CloudStorage.SaveStorage();
            return new Table(name);
        }

        /// <summary>
        /// 判断给出的表是否存在于当前数据库中
        /// </summary>
        /// <param name="name">表名</param>
        /// <returns>是否存在</returns>
        public bool tableExists(string name)
        {
            return this.tableNameList.Contains(name);
        }

        /// <summary>
        /// 删除一个已经存在的表
        /// </summary>
        /// <param name="name">要删除的表名</param>
        public void dropTable(string name)
        {
            if (! tableExists(name))
            {
                throw new Exception(String.Format("表{0}不存在.", name));
            }
            //先移除cell
            long cellId = tableIdList.ElementAt(tableNameList.IndexOf(name));
            Global.CloudStorage.RemoveCell(cellId);
            //再移除数据库内信息
            tableNameList.Remove(name);
            tableIdList.Remove(cellId);
            //再同步云端的数据库
            UpdateDatabaseMessageWriter udmw = new UpdateDatabaseMessageWriter(
                name: this.name, tableNameList: this.tableNameList, tableIdList: this.tableIdList);
            int serverId = Global.CloudStorage.GetServerIdByCellId(HashHelper.HashString2Int64(this.name));
            Global.CloudStorage.UpdateDatabaseToDatabaseServer(serverId, udmw);
            Global.CloudStorage.SaveStorage();
        }

        /// <summary>
        /// 获得当前使用的数据库
        /// </summary>
        /// <returns>当前使用的数据库对象</returns>
        public static Database getCurrentDatabase()
        {
            return currentDatabase;
        }

        public List<long> getTableIdList()
        {
            return this.tableIdList;
        }

        public List<string> getTableNameList()
        {
            return this.tableNameList;
        }

        public string getName()
        {
            return this.name;
        }
    }
}
