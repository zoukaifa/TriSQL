using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;
using Trinity;
using Trinity.Core.Lib;
using System.Threading;

namespace TriSQLApp
{
    struct dint
    {
        public int a;
        public int b;
        public dint(int a, int b)
        {
            this.a = a;
            this.b = b;
        }
    }
    class Table
    {
        //类成员的初始化在构造方法里进行
        private bool isSingle;  //是否是直接由构造函数生成的完整单表（即使是select的也是false）
        private List<List<long>> cellIds = new List<List<long>>();
        private List<int> columnTypes = new List<int> { };
        private List<string> columnNames = new List<string> { };
        private List<int> primaryIndexs = new List<int> { };  //主键索引
        private List<Element> defaultValues = new List<Element> { };  //默认值
        private List<string> tableNames = new List<string> { };
        public struct UpdateMessage
        {
            public List<long> cellId;
            public string fieldname;
            public int flag;
            public char op;
            public int operationNum;
            public Condition con;
            public List<int> typeList;
        }
        public Table(List<List<long>> cellIds)
        {
            this.cellIds = cellIds;
        }
       
        public Table(List<List<long>> cellIds, List<int> columnTypes, List<string> columnNames,
                List<int> primaryIndexs, List<Element> defaultValues, List<string> tableNames)
        {
            this.cellIds = cellIds;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
            this.primaryIndexs = primaryIndexs;
            this.defaultValues = defaultValues;
            this.tableNames = tableNames;
        }

        public Table(List<string> columnNameList, List<int> columnTypeList)
        {
            this.columnNames = columnNameList;
            this.columnTypes = columnTypeList;
        }

        
        public Table()
        {
            cellIds = new List<List<long>>();
            columnNames = new List<string>();
            columnTypes = new List<int>();
            primaryIndexs = new List<int>();
            defaultValues = new List<Element>();
        }
        public Table(params string[] tableName)
        {
            List<long> tableIds = new List<long> { };
            if (Database.getCurrentDatabase() == null)
            {
                throw new Exception(String.Format("当前数据库不存在"));
            }

            if (tableName.Length == 1)
            {
                isSingle = true;
                if (!Database.getCurrentDatabase().tableExists(tableName[0]))
                {
                    throw new Exception(String.Format("当前表{0}不存在!", tableName[0]));
                }
                this.tableNames.Add(tableName[0]);
                tableIds.Add(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableName
                    [0])));
                using (var request = new GetTableMessageWriter(tableIds[0]))
                {
                    int serverId = Global.CloudStorage.GetServerIdByCellId(tableIds[0]);
                    using (var res = Global.CloudStorage.GetTableToDatabaseServer(serverId, request))
                    {
                        this.cellIds = res.cellIds;
                        this.columnNames = res.columnNameList;
                        this.columnTypes = res.columnTypeList;
                        this.defaultValues = res.defaultValue;
                        this.primaryIndexs = res.primaryIndex;
                    }
                }
            }
            else
            {
                isSingle = false;
            }
        }
        public void delete(string con)
        {
            if (!isSingle)
            {
                throw new Exception(String.Format("不可对多个表进行delete操作"));
            }
            DeleteMessageWriter dmw = new DeleteMessageWriter(cellIds, this.columnTypes, con);
            List<List<long>> newCellIds = Global.CloudStorage.DeleteFromClientToDatabaseProxy(0, dmw).cellIds;
            foreach (List<long> ids in newCellIds)
            {
                this.cellIds.Remove(ids);
            }
            TableHeadCell thc = new TableHeadCell(this.tableNames[0], this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            long thcId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                   [0]));
            Global.CloudStorage.SaveTableHeadCell(thcId, thc);
        }
        public void truncate()
        {
            if (!isSingle)
            {
                throw new Exception(String.Format("不可对多个表进行delete操作"));
            }

            TruncateMessageWriter tmw = new TruncateMessageWriter(cellIds);
            Global.CloudStorage.TruncateFromClientToDatabaseProxy(0, tmw);

            this.cellIds = null;
            TableHeadCell thc = new TableHeadCell(this.tableNames[0], this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            long thcId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                   [0]));
            Global.CloudStorage.SaveTableHeadCell(thcId, thc);
        }
        public void update(string fieldName, int flag, char op, int opNum, string con)
        {
            Table table = new Table(this.cellIds);
            Condition contemp = new Condition(table, con);
            List<Thread> threads = new List<Thread> { };
            foreach (List<long> Id in this.cellIds)
            {
                UpdateMessage um = new UpdateMessage();
                um.con = contemp;
                um.fieldname = fieldName;
                um.flag = flag;
                um.op = op;
                um.operationNum = opNum;
                um.cellId = Id;
                um.typeList = this.columnTypes;
                Thread thread = new Thread(new ParameterizedThreadStart(UpdateFunction));
                threads.Add(thread);
                thread.Start(um);
            }
            foreach (Thread thr in threads)
            {
                thr.Join();
            }
        }
        private void UpdateFunction(Object Message)
        {
            UpdateMessage um = (UpdateMessage)Message;

            List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                Global.CloudStorage.GetServerIdByCellId(um.cellId[0]),
                new GetRowMessageWriter(um.cellId)).row;
            List<Object> values = FieldType.getValues(row, um.typeList);
            int index = this.columnNames.IndexOf(um.fieldname);
            int serverID;
            if (um.con.getResult(values))//um.con.getResult(values)
            {
                Element ele = new Element { };
                using (var req = new GetElementMessageWriter(um.cellId[index]))
                {
                    serverID = Global.CloudStorage.GetServerIdByCellId(um.cellId[index]);
                    using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID, req))
                    {
                        ele = responce.ele;
                    }
                }
                if (um.flag == 1)
                {
                    switch (um.op)
                    {
                        case '+':
                            ele.intField += um.operationNum;
                            break;
                        case '-':
                            ele.intField -= um.operationNum;
                            break;
                        case '*':
                            ele.intField *= um.operationNum;
                            break;
                        case '/':
                            ele.intField /= um.operationNum;
                            break;
                        default:
                            throw new Exception(String.Format("不合法的操作"));
                    }
                }
                else
                {
                    switch (um.op)
                    {
                        case '+':
                            ele.intField += um.operationNum;
                            break;
                        case '-':
                            ele.intField = um.operationNum - ele.intField;
                            break;
                        case '*':
                            ele.intField *= um.operationNum;
                            break;
                        case '/':
                            ele.intField = um.operationNum / ele.intField;
                            break;
                        default:
                            throw new Exception(String.Format("不合法的操作"));
                    }
                }
                ElementCell eleCell = FieldType.getElementCell(ele);
                Global.CloudStorage.SaveElementCell(um.cellId[index], eleCell);
            }
        }

        public void insert(string[] fieldNames, object[] values)
        {
            if (!isSingle)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }

            List<Element> ele = new List<Element>();
            List<long> ID = new List<long> { };
            ElementCell elecell = FieldType.setValueCell(values[0], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[0])));
            Global.CloudStorage.SaveElementCell(elecell);
            ID.Add(elecell.CellID);
            for (int i = 1; i < fieldNames.Length; i++)
            {
                Element temp = FieldType.setValue(values[i], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[i])));
                ele.Add(temp);
            }
            using (var request = new InsertMessageWriter(ele))
            {
                int serverId = Global.CloudStorage.GetServerIdByCellId(elecell.CellID);
                using (var res = Global.CloudStorage.InsertElementToDatabaseServer(serverId, request))
                {
                    for (int k = 0; k < res.cellIds.Count; k++)
                    {
                        ID.Add(res.cellIds[k]);
                    }
                }
            }
            this.cellIds.Add(ID);
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                 [0]));
            TableHeadCell thc = new TableHeadCell(this.tableNames[0], this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
        }

        public void insert(string[] fieldNames, object[][] values)
        {
            if (tableNames.Count > 1)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            for (int j = 0; j < values.Length; j++)
            {
                List<Element> ele = new List<Element>();
                List<long> ID = new List<long> { };
                ElementCell elecell = FieldType.setValueCell(values[j][0], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[0])));
                Global.CloudStorage.SaveElementCell(elecell);
                ID.Add(elecell.CellID);
                for (int i = 1; i < fieldNames.Length; i++)
                {
                    Element temp = FieldType.setValue(values[j][i], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[i])));
                    ele.Add(temp);
                }
                using (var request = new InsertMessageWriter(ele))
                {
                    int serverId = Global.CloudStorage.GetServerIdByCellId(elecell.CellID);
                    using (var res = Global.CloudStorage.InsertElementToDatabaseServer(serverId, request))
                    {
                        for (int k = 0; k < res.cellIds.Count; k++)
                        {
                            ID.Add(res.cellIds[k]);
                        }
                    }
                }
                this.cellIds.Add(ID);
            }
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                [0]));
            TableHeadCell thc = new TableHeadCell(this.tableNames[0], this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);

        }
        public void insert(string[] fieldNames, Table anotherTable)
        {
            if (this.tableNames.Count != 1)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            for (int i = 0; i < anotherTable.cellIds.Count; i++)
            {
                this.cellIds.Add(anotherTable.cellIds[i]);
            }
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
               [0]));
            TableHeadCell thc = new TableHeadCell(this.tableNames[0], this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
        }
        public void rename(string newName)
        {
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                  [0]));
            TableHeadCell thc = new TableHeadCell(newName, this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
            //更改数据库里的tablelist信息
            List<string> tbName = Database.getCurrentDatabase().getTableNameList();
            List<long> tbID = Database.getCurrentDatabase().getTableIdList();
            tbName.RemoveAt(tbID.IndexOf(tableId));
            tbID.Remove(tableId);
            tbName.Add(newName);
            tbID.Add(tableId);
            long dbId = HashHelper.HashString2Int64(Database.getCurrentDatabase().getName());
            DatabaseCell dbc = new DatabaseCell(Database.getCurrentDatabase().getName(), tbName, tbID);
            Global.CloudStorage.SaveDatabaseCell(dbId, dbc);
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
            List<string> newColumnNames = new List<string>();
            List<int> newColumnTypes = new List<int>();
            List<int> usedIndexes = new List<int>();  //标记被select的索引
            //先检查字段是否符合
            if (fields.Length == 1 && fields[0].Item1.Equals("*"))
            {
                newColumnNames = columnNames;
                newColumnTypes = columnTypes;
            }
            else
            {
                foreach (Tuple<string, string> field in fields)
                {
                    string fieldName = field.Item1;  //字段名
                    if (columnNames.Contains(fieldName))  //直接能识别出该字段
                    {
                        newColumnNames.Add((field.Item2 == null) || (field.Item2.Equals("")) ?
                            fieldName : field.Item2);  //别名
                        usedIndexes.Add(columnNames.IndexOf(fieldName));
                    }
                    else  //说明要么该字段不存在，要么没有使用表名直接使用字段名
                    {
                        int count = 0;
                        int fieldIndex = 0;
                        for (int i = 0; i < columnNames.Count; i++)
                        {
                            if (columnNames.Contains("." + fieldName))  //统计是否重复了
                            {
                                count++;
                                fieldIndex = i;
                            }
                        }
                        if (count == 0)
                        {
                            throw new Exception(String.Format("字段{0}不存在", fieldName));
                        }
                        else if (count > 1)
                        {
                            throw new Exception(String.Format("字段{0}重复，需要指明表名", fieldName));
                        }
                        else
                        {
                            newColumnNames.Add((field.Item2 == null) || (field.Item2.Equals("")) ?
                            columnNames[fieldIndex] : field.Item2);  //别名
                            usedIndexes.Add(fieldIndex);
                        }
                    }
                }
            }
            usedIndexes.Sort();
            //此时，新表的结构构建完成，向proxy发送查询任务
            SelectMessageWriter smw = new SelectMessageWriter(columnNames, columnTypes, cellIds, usedIndexes, con);
            List<List<long>> newCellIds = Global.CloudStorage.SelectFromClientToDatabaseProxy(0, smw).cellIds;
            return new Table(newCellIds, newColumnTypes, newColumnNames, this.primaryIndexs,
                this.defaultValues, this.tableNames);
        }

        /// <summary>
        /// 获得某一行
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public List<Object> getRow(int index)
        {
            List<long> cellId = cellIds[index];
            List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                Global.CloudStorage.GetServerIdByCellId(cellId[0]),
                new GetRowMessageWriter(cellId)).row;
            return FieldType.getValues(row, columnTypes);
        }

        /// <summary>
        /// 获得某一列
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public List<Object> getColumn(string name)
        {
            int index = 0;
            if (columnNames.Contains(name))  //直接能识别出该字段
            {
                index = columnNames.IndexOf(name);
            }
            else  //说明要么该字段不存在，要么没有使用表名直接使用字段名
            {
                int count = 0;
                for (int i = 0; i < columnNames.Count; i++)
                {
                    if (columnNames.Contains("." + name))  //统计是否重复了
                    {
                        count++;
                        index = i;
                    }
                }
                if (count == 0)
                {
                    throw new Exception(String.Format("字段{0}不存在", name));
                }
                else if (count > 1)
                {
                    throw new Exception(String.Format("字段{0}重复，需要指明表名", name));
                }
            }
            List<Object> result = new List<object>();
            foreach (List<long> rowId in cellIds)
            {
                result.Add(FieldType.getValue(
                    Global.CloudStorage.GetElementToDatabaseServer(
                        Global.CloudStorage.GetServerIdByCellId(rowId[index]),
                        new GetElementMessageWriter(rowId[index])).ele, columnTypes[index]));
            }
            return result;
        }

        public List<List<long>> getCellIds()
        {
            return this.cellIds;
        }

        public List<string> getColumnNames()
        {
            return columnNames;
        }

        public List<int> getColumnTypes()
        {
            return columnTypes;
        }

        public void printTable()
        {
            foreach(string name in columnNames)
            {
                Console.Write("{0, -15}", name);
            }
            Console.WriteLine();
            for(int i = 0; i < cellIds.Count; i++)
            {
                List<Object> row = getRow(i);
                foreach(Object ele in row)
                {
                    Console.Write("{0, -15}", ele);
                }
                Console.WriteLine();
            }
        }
    }

}