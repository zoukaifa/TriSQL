using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;
using Trinity;
using Trinity.Core.Lib;

namespace TriSQLApp
{
    class Table
    {
        //类成员的初始化在构造方法里进行
        private bool isSingle;  //是否是直接由构造函数生成的完整单表（即使是select的也是false）
        private List<List<long>> cellIds;
        private List<int> columnTypes;
        private List<string> columnNames;
        private List<int> primaryIndexs;  //主键索引
        private List<Element> defaultValues;  //默认值


        public List<List<string>> ColumnNames
        {
            get
            {
                return columnNames;
            }
        }

        public List<List<int>> Indexes
        {
            get
            {
                return indexes;
            }
        }

        public List<string> TableNames
        {
            get
            {
                return tableNames;
            }
        }

        public List<long> TableIds
        {
            get
            {
                return tableIds;
            }
        }

        public List<List<long>> CellIds
        {
            get
            {
                return cellIds;
            }
        }

        public List<List<int>> PrimaryIndexs
        {
            get
            {
                return primaryIndexs;
            }
        }

        public List<List<Element>> DefaultValues
        {
            get
            {
                return defaultValues;
            }
        }

        public Table(params string[] tableNames)
        {
            //判断当前database是不是存在的
            if (Database.getCurrentDatabase() == null)
            {
                throw new Exception(String.Format("当前数据库不存在"));
            }
            this.databaseName = Database.getCurrentDatabase().getName();
            if (tableNames.Length == 1)
            {
                if (!Database.getCurrentDatabase().tableExists(tableNames[0]))
                {
                    throw new Exception(String.Format("当前表{0}不存在!", tableNames[0]));
                }
                //获取当前表的名字
                //this.tableNames[0] = tableNames[0];
                this.tableNames.Add(tableNames[0]);
                //获取表的ID
                // this.tableIds[0] = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                //     [0]));
                this.tableIds.Add(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
                using (var request = new GetTableMessageWriter(Database.getCurrentDatabase().getName(), tableIds[0]))
                {
                    int serverId = Global.CloudStorage.GetServerIdByCellId(tableIds[0]);
                    using (var res = Global.CloudStorage.GetTableToDatabaseServer(serverId, request))
                    {
                        // this.columnNames[0] = res.columnNameList;
                        this.columnNames.Add(res.columnNameList);
                        //this.cellIds[0] = res.rowList;
                        this.cellIds.Add(res.rowList);
                        //this.columnTypes[0] = res.columnTypeList;
                        this.columnTypes.Add(res.columnTypeList);
                        //this.primaryIndexs[0] = res.primaryIndex;
                        this.primaryIndexs.Add(res.primaryIndex);
                        //this.defaultValues[0] = res.defaultValue;
                        this.defaultValues.Add(res.defaultValue);
                    }
                }

            }
            else if (tableNames.Length > 1)
            {
                int sum = 1;
                List<int> countNum = new List<int> { };
                List<int> Num = new List<int> { };
                List<List<long>> CIDs = new List<List<long>> { };

                for (int i = 0; i < tableNames.Length; i++)
                {
                    if (!Database.getCurrentDatabase().tableExists(tableNames[i]))
                    {
                        throw new Exception(String.Format("当前表{0}不存在!", tableNames[i]));
                    }
                    this.tableNames.Add(tableNames.ElementAt(i));
                    Console.WriteLine("{0}", this.tableNames[i]);
                    // this.tableIds[i]= Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    // [i]));
                    this.tableIds.Add(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                   [i])));
                    Console.WriteLine("{0}", this.tableIds.ElementAt(i));
                    using (var request = new GetTableMessageWriter(Database.getCurrentDatabase().getName(), tableIds[i]))
                    {
                        //int serverId = Global.CloudStorage.GetServerIdByCellId(tableIds[i]);
                        int serverId = Global.CloudStorage.GetServerIdByCellId(tableIds[i]);
                        using (var res = Global.CloudStorage.GetTableToDatabaseServer(serverId, request))
                        {
                            // this.columnNames[0] = res.columnNameList;
                            this.columnNames.Add(res.columnNameList);
                            //this.cellIds[0] = res.rowList;
                            // this.cellIds.Add(res.rowList);
                            //this.columnTypes[0] = res.columnTypeList;
                            this.columnTypes.Add(res.columnTypeList);
                            //this.primaryIndexs[0] = res.primaryIndex;
                            this.primaryIndexs.Add(res.primaryIndex);
                            //this.defaultValues[0] = res.defaultValue;
                            this.defaultValues.Add(res.defaultValue);
                            countNum.Add(res.rowList.Count);
                            CIDs.Add(res.rowList);
                        }
                    }
                }
                for (int i = 0; i < tableNames.Length; i++)
                {
                    sum = sum * countNum[i];

                }
                for (int j = 0; j < tableNames.Length; j++)
                {
                    int tempNum = 1;
                    for (int i = tableNames.Length - 1; i > j; i--)
                    {
                        tempNum = tempNum * countNum[i];
                    }
                    Num.Add(tempNum);
                }
                Console.WriteLine("{0}", sum);
                List<long> temp = new List<long> { };
                for (int i = 0; i < tableNames.Length; i++)
                {
                    List<long> tempList = new List<long> { };
                    for (int j = 0; j < sum / (Num[i] * countNum[i]); j++)
                    {
                        for (int k = 0; k < countNum[i]; k++)
                        {
                            for (int l = 0; l < Num[i]; l++)
                            {
                                tempList.Add(CIDs[i][k]);
                            }
                        }
                    }
                    this.cellIds.Add(tempList);
                }
            }
        }


        public Table(List<List<long>> cellIds,
                     List<string> tableNames,
                     List<long> tableIds,
                     List<List<int>> indexes,
                     List<List<int>> columnTypes,
                     List<List<string>> columnNames,
                     List<List<int>> primaryIndexs,
                     List<List<Element>> defaultValues
                     )
        {
            this.cellIds = cellIds;
            this.tableIds = tableIds;
            this.tableNames = tableNames;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
            this.indexes = indexes;
            this.primaryIndexs = primaryIndexs;
            this.defaultValues = defaultValues;
        }
        public void delete(Condition con = null)
        {
            List<long> RID = con.getResult(this.tableNames, con);
            if (tableNames.Count != 1)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            for (int i = 0; i < RID.Count; i++)
            {
                for (int j = 0; j < this.cellIds[0].Count; j++)
                {
                    if (this.cellIds[0][j] == RID[i])
                    {
                        this.cellIds[0].RemoveAt(j);
                    }
                }
            }
            UpdateTableMessageWriter utmw = new UpdateTableMessageWriter(tableName: this.tableNames[0], tableId: this.tableIds[0], columnNameList: this.columnNames[0], columnTypeList: this.columnTypes
              [0], primaryIndex: this.primaryIndexs[0], defaultValue: this.defaultValues[0], rowList: this.cellIds[0]);
            int serverID = Global.CloudStorage.GetServerIdByCellId(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
            Global.CloudStorage.UpdateTableToDatabaseServer(serverID, utmw);
            Global.CloudStorage.SaveStorage();
        }


        public void insert(string[] fieldNames, object[] values)
        {
            if (tableNames.Count != 1)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            RowCell rc = new RowCell(values: new List<Element>());
            for (int j = 0; j < fieldNames.Length; j++)
            {
                if (!this.columnNames[0].Contains(fieldNames[j]))
                {
                    throw new Exception(String.Format("当前字段不存在"));
                }
                object val = values[j];
                int type = columnTypes[0].ElementAt(columnNames[0].IndexOf(fieldNames[j]));
                rc.values.Add(FieldType.setValue(val, type));
            }
            this.cellIds[0].Add(rc.CellID);
            Global.CloudStorage.SaveRowCell(rc);
            UpdateTableMessageWriter utmw = new UpdateTableMessageWriter(tableName: tableNames[0], tableId: this.tableIds[0], columnNameList: this.columnNames[0], columnTypeList: this.columnTypes
                [0], primaryIndex: this.primaryIndexs[0], defaultValue: this.defaultValues[0], rowList: this.cellIds[0]);
            int serverID = Global.CloudStorage.GetServerIdByCellId(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
            Global.CloudStorage.UpdateTableToDatabaseServer(serverID, utmw);
            //Global.CloudStorage.SaveStorage();
        }

        public void insert(string[] fieldNames, object[][] values)
        {
            if (tableNames.Count != 1)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            for (int i = 0; i < values.Length; i++)
            {
                RowCell rc = new RowCell(values: new List<Element>());
                for (int j = 0; j < fieldNames.Length; j++)
                {
                    if (!this.columnNames[0].Contains(fieldNames[j]))
                    {
                        throw new Exception(String.Format("当前字段不存在"));
                    }
                    object val = values[i][j];
                    int type = columnTypes[0].ElementAt(columnNames[0].IndexOf(fieldNames[j]));
                    rc.values.Add(FieldType.setValue(val, type));
                }
                this.cellIds[0].Add(rc.CellID);
                Global.CloudStorage.SaveRowCell(rc);
            }

            UpdateTableMessageWriter utmw = new UpdateTableMessageWriter(tableName: tableNames[0], tableId: this.tableIds[0], columnNameList: this.columnNames[0], columnTypeList: this.columnTypes
                [0], primaryIndex: this.primaryIndexs[0], defaultValue: this.defaultValues[0], rowList: this.cellIds[0]);

            int serverID = Global.CloudStorage.GetServerIdByCellId(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
            Global.CloudStorage.UpdateTableToDatabaseServer(serverID, utmw);
            Global.CloudStorage.SaveStorage();
        }


        public void truncate()
        {
            List<long> RID = null;
            //remove cell
            UpdateTableMessageWriter utmw = new UpdateTableMessageWriter(tableName: tableNames[0], tableId: this.tableIds[0], columnNameList: this.columnNames[0], columnTypeList: this.columnTypes
                [0], primaryIndex: this.primaryIndexs[0], defaultValue: this.defaultValues[0], rowList: RID);

            int serverID = Global.CloudStorage.GetServerIdByCellId(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
            Global.CloudStorage.UpdateTableToDatabaseServer(serverID, utmw);
            Global.CloudStorage.SaveStorage();
        }

        public void rename(string newName)
        {
            UpdateTableMessageWriter utmw = new UpdateTableMessageWriter(tableName: newName, tableId: this.tableIds[0], columnNameList: this.columnNames[0], columnTypeList: this.columnTypes
               [0], primaryIndex: this.primaryIndexs[0], defaultValue: this.defaultValues[0], rowList: this.cellIds[0]);
            int serverID = Global.CloudStorage.GetServerIdByCellId(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                    [0])));
            Global.CloudStorage.UpdateTableToDatabaseServer(serverID, utmw);
            Global.CloudStorage.SaveStorage();
            //更改数据库里的tablelist信息
            List<string> tbName = Database.getCurrentDatabase().getTableNameList();
            List<long> tbID = Database.getCurrentDatabase().getTableIdList();
            tbName.RemoveAt(tbID.IndexOf(this.tableIds[0]));
            tbID.Remove(this.tableIds[0]);
            tbName.Add(newName);
            tbID.Add(this.tableIds[0]);
            UpdateDatabaseMessageWriter udmw = new UpdateDatabaseMessageWriter(
               name: Database.getCurrentDatabase().getName(), tableNameList: tbName, tableIdList: tbID);
            int dbId = Global.CloudStorage.GetServerIdByCellId(HashHelper.HashString2Int64(Database.getCurrentDatabase().getName()));
            Global.CloudStorage.UpdateDatabaseToDatabaseServer(dbId, udmw);
            Global.CloudStorage.SaveStorage();
        }

        public Table union(Table anotherTable)
        {
            return null;
        }

        public Table innerJoin(Table anotherTable)
        {
            return null;
        }

        /// <summary>
        /// 无into的select语句
        /// </summary>
        /// <param name="fields">三元组数组，第一个表示字段，第二个表示别名</param>
        /// <param name="con">条件表达式</param>
        /// <returns></returns>
        public Table select(Tuple<string, string>[] fields, string con)
        {
            //新表的结构
            List<List<long>> newCellIds = new List<List<long>>();
            List<string> newTableNames = new List<string>();
            List<long> newTableIds = new List<long>();
            List<List<int>> newIndexes = new List<List<int>>();
            List<List<string>> newColumnNames = new List<List<string>>();
            List<List<int>> newColumnTypes = new List<List<int>>();
            //先检查字段是否符合
            if (fields.Length == 1 && fields[0].Item1.Equals("*"))
            {
                newTableIds = tableIds;
                newTableNames = tableNames;
                newIndexes = indexes;
                newColumnNames = columnNames;
                newColumnTypes = columnTypes;
            }
            else
            {
                foreach (Tuple<string, string> field in fields)
                {
                    string fieldName = field.Item1;  //字段名
                    if (fieldName.Contains("."))  //表名.字段名
                    {
                        string tableName = fieldName.Split('.')[0];
                        fieldName = fieldName.Split('.')[1];
                        int tableIndex = tableNames.IndexOf(tableName);
                        if (tableIndex != -1)  //含有该表
                        {
                            if (!newTableNames.Contains(tableName))
                            {  //添加到新表的表结构里
                                newTableNames.Add(tableName);
                                newTableIds.Add(tableIds[newTableNames.IndexOf(tableName)]);
                                newColumnNames.Add(new List<string>());
                                newIndexes.Add(new List<int>());
                                newColumnTypes.Add(new List<int>());
                            }
                            int newTableIndex = newTableNames.IndexOf(tableName);
                            int fieldIndex = columnNames[tableIndex].IndexOf(fieldName);
                            if (fieldIndex == -1)
                            {
                                throw new Exception(String.Format("表{0}不存在字段{1}", tableName, fieldName));
                            }
                            else
                            {
                                //此时确认表和字段均存在
                                string newFieldName = field.Item2 == null ? fieldName : field.Item2;  //更名
                                newColumnNames[newTableIndex].Add(newFieldName);  //完善新的表结构
                                newIndexes[newTableIndex].Add(indexes[tableIndex][fieldIndex]);
                                newColumnTypes[newTableIndex].Add(columnTypes[tableIndex][fieldIndex]);
                            }

                        }
                        else
                        {
                            throw new Exception(String.Format("表{0}不存在.", tableName));
                        }
                    }
                }
            }
            //此时，新表的结构构建完成，向proxy发送查询任务
            SelectMessageWriter smw = new SelectMessageWriter(cellIds, tableNames, tableIds, indexes,
                columnTypes, columnNames, primaryIndexs, defaultValues, con);
            using (var response = Global.CloudStorage.SelectFromClientToDatabaseProxy(0, smw))
            {
                //现在筛选在客户端重新排列rowIds
                List<List<long>> rowIds = response.rowIds;
                //横向的顺序需要按照newTableNames的顺序来排列
                for (int i = 0; i < newTableNames.Count; i++)
                {
                    newCellIds.Add(rowIds[tableNames.IndexOf(newTableNames[i])]);
                }
            }
            return new Table(newCellIds, newTableNames, newTableIds, newIndexes, newColumnTypes, newColumnNames,
                primaryIndexs, defaultValues);
        }

        /// <summary>
        /// 有into的select
        /// </summary>
        /// <param name="fields">元组第一个表示字段或表达式，第二个表示施加的函数</param>
        /// <param name="con">条件</param>
        /// <param name="vars">要into的变量</param>
        public void select(Tuple<string, int>[] fields, string con, ref object[] vars)
        {
            
        }

      
        public void print()
        {
            for(int i = 0; i<tableNames.Count; i++)
            {
                for (int j = 0; j < columnNames[i].Count; j++)
                {
                    Console.Write("{0, -16}", tableNames[i]+"."+columnNames[i][j]);
                }
            }
            Console.WriteLine();
            for(int index = 0; index<cellIds[0].Count;index++)
            {

                List<List<Object>> row = getRow(index);
                for (int i = 0; i < tableNames.Count; i++)
                {
                    for (int j = 0; j < columnNames[i].Count; j++)
                    {
                        Console.Write("{0, -16}", row[i][j]);
                    }
                }
                Console.WriteLine();
            }
        }
        public List<List<Object>> getRow(int index)
        {
            List<List<Object>> row = new List<List<object>>();
            for(int i = 0; i < cellIds.Count; i++)
            {
                long cellId = cellIds[i][index];
                Console.WriteLine(Global.CloudStorage.GetServerIdByCellId(cellId));
                List<Element> value = Global.CloudStorage.GetRowToDatabaseServer(
                        Global.CloudStorage.GetServerIdByCellId(cellId),
                        new GetRowMessageWriter(cellId)).row;
                row.Add(FieldType.getValues(value, columnTypes[i]));
            }
            return row;
        }
    }
}
