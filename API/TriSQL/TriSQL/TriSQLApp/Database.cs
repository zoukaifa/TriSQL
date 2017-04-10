﻿using System;
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
                        this.tableIdList = resp.tableList;
                        this.tableNameList = resp.tableNameList;
                        break;
                    }
                }
            }
            
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

            DatabaseCell dbc = new DatabaseCell(name: name, tableList: new List<long>(),
                tableNameList: new List<string>());
            //以数据库名的hash为cellId，存入云端
            Global.CloudStorage.SaveDatabaseCell(HashHelper.HashString2Int64(name), dbc);
            Global.CloudStorage.SaveStorage();
            Database database = new Database(name, dbc.tableList, dbc.tableNameList);
            return database;
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
