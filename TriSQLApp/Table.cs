using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;
using Trinity;
using Trinity.Core.Lib;
using System.Threading;
using System.Collections.Concurrent;
namespace TriSQLApp
{
    class dint : IComparable<dint>
    {
        public int a;
        public int b;
        public dint(int a, int b)
        {
            this.a = a;
            this.b = b;
        }
        public dint() { }
        public int CompareTo(dint p)
        {
            return this.b.CompareTo(p.b);
        }
    }
    class Table
    {
        #region 字段
        //类成员的初始化在构造方法里进行
        private bool isSingle = false;  //是否是直接由构造函数生成的完整单表（即使是select的也是false）
        private List<List<long>> cellIds = new List<List<long>>();
        private List<int> columnTypes = new List<int> { };
        private List<string> columnNames = new List<string> { };
        private List<int> primaryIndexs = new List<int> { };  //主键索引
        private List<Element> defaultValues = new List<Element> { };  //默认值
        //private List<string> tableNames = new List<string> { };
        private string TableName;
        public struct UpdateMessage
        {
            public int threadCount;
            public int threadIndex;
            public string fieldname;
            public int flag;
            public char op;
            public object operationNum;
            public string con;
            public UpdateMessage(int threadCount, int threadIndex, string fieldName, int flag,
                char op, object operationNum, string con)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.fieldname = fieldName;
                this.flag = flag;
                this.op = op;
                this.operationNum = operationNum;
                this.con = con;
            }
        }
        public struct UpdateFields
        {
            public int threadCount;
            public int threadIndex;
            public string[] fildNames;
            public int Value1;
            public double Value2;
            public string con;
            public UpdateFields(int threadCount, int threadIndex, string[] fieldNames, int Value1,
                double Value2, string con)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.fildNames = fieldNames;
                this.Value1 = Value1;
                this.Value2 = Value2;
                this.con = con;
            }
        }
        #endregion
        #region 构造
        public Table(List<List<long>> cellIds)
        {
            this.cellIds = cellIds;
        }
        public Table(List<List<long>> cellIds, List<int> columnTypes, List<string> columnNames)
        {
            this.cellIds = cellIds;
            this.columnTypes = columnTypes;
            this.columnNames = columnNames;
        }
        public Table(List<List<long>> cellIds, List<int> columnTypes, List<string> columnNames,
                List<int> primaryIndexs, List<Element> defaultValues, string TableName)
        {
            this.cellIds = cellIds;
            this.columnNames = columnNames;
            this.columnTypes = columnTypes;
            this.primaryIndexs = primaryIndexs;
            this.defaultValues = defaultValues;
            this.TableName = TableName;
        }

        public Table(List<string> columnNameList, List<int> columnTypeList)
        {
            this.columnNames = columnNameList;
            this.columnTypes = columnTypeList;
        }
        public Table(Table oldtable, String name)
        {
            this.cellIds = oldtable.cellIds;
            this.columnNames = oldtable.columnNames;
            this.columnTypes = oldtable.columnTypes;
            this.defaultValues = oldtable.defaultValues;
            this.primaryIndexs = oldtable.primaryIndexs;
            this.isSingle = oldtable.isSingle;
            this.TableName = name;
        }
        public Table()
        {
            cellIds = new List<List<long>>();
            columnNames = new List<string>();
            columnTypes = new List<int>();
            primaryIndexs = new List<int>();
            defaultValues = new List<Element>();
        }
        public Table(string tableName)
        {
            List<long> tableIds = new List<long> { };
            if (Database.getCurrentDatabase() == null)
            {
                throw new Exception(String.Format("当前数据库不存在"));
            }


            isSingle = true;
            if (!Database.getCurrentDatabase().tableExists(tableName))
            {
                throw new Exception(String.Format("当前表{0}不存在!", tableName[0]));
            }
            tableIds.Add(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableName)));
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
                    this.TableName = tableName;
                }
            }
        }
        public static Table MultiTable(String name, params string[] tableName)
        {
            Table M;
            Table t1 = new Table(tableName[0]);
            Table t2 = new Table(tableName[1]);
            M = t1.innerJoinOnCluster(t2, name, new List<dint>());
            for (int i = 2; i < tableName.Length; i++)
            {
                Table temp = new Table(tableName[i]);
                M = M.innerJoinOnCluster(temp, name, new List<dint>());
            }
            return M;
        }
        #endregion
        #region 田超-delete,update,insert,truncate,rename
        public int IndexOfCell(List<List<long>> cells, List<long> tempList)
        {
            int index = -1;

            for (int i = 0; i < cells.Count; i++)
            {
                int flag = 1;
                for (int j = 0; j < tempList.Count; j++)
                {
                    if (!tempList[j].Equals(cells[i][j]))
                    {
                        flag = 0;
                        break;
                    }

                }
                if (flag == 1)
                {
                    index = i;
                    break;
                }

            }
            return index;
        }
        public static void CopyValue(List<List<long>> origin, List<List<long>> target)
        {

            System.Reflection.PropertyInfo[] properties = (target.GetType()).GetProperties();
            System.Reflection.FieldInfo[] fields = (origin.GetType()).GetFields();
            for (int i = 0; i < fields.Length; i++)
            {
                for (int j = 0; j < properties.Length; j++)
                {
                    if (fields[i].Name == properties[j].Name && properties[j].CanWrite)
                    {
                        properties[j].SetValue(target, fields[i].GetValue(origin), null);
                    }
                }
            }

        }

        private struct DeleteMessage
        {
            public int threadCount;
            public int threadIndex;
            public Condition con;
            public ConcurrentBag<List<long>> result;
            public DeleteMessage(int threadCount, int threadIndex, Condition con, ConcurrentBag<List<long>> result)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.con = con;
                this.result = result;
            }
        }
        private void deleteThread(Object deleteMessage)
        {
            DeleteMessage dm = (DeleteMessage)deleteMessage;
            int start = -1;
            int end = -1;
            int c = cellIds.Count;
            int el = c / dm.threadCount;
            if (dm.threadCount != dm.threadIndex + 1)
            {
                start = dm.threadIndex * el;
                end = start + el - 1;
            }
            else
            {
                start = dm.threadIndex * el;
                end = c - 1;
            }
            for (int i = start; i <= end; i++)
            {
                List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                    Global.CloudStorage.GetServerIdByCellId(cellIds[i][0]),
                    new GetRowMessageWriter(cellIds[i])).row;
                List<Object> values = FieldType.getValues(row, columnTypes);
                if (dm.con.getResult(values))
                {
                    dm.result.Add(cellIds[i]);
                    for (int j = 0; j < values.Count; j++)
                    {
                        if (Global.CloudStorage.Contains(cellIds[i][j])) {
                            Global.CloudStorage.RemoveCell(cellIds[i][j]);
                        }
                    }
                }
            }
        }
        public void delete(string con)
        {
            if (!isSingle)
            {
                throw new Exception(String.Format("不可对多个表进行delete操作"));
            }
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            Condition cond = new Condition(this, con);
            ConcurrentBag<List<long>> result = new ConcurrentBag<List<long>>();
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                DeleteMessage dm = new DeleteMessage(threadCount, threadIndex, cond, result);
                threadNum[threadIndex] = new Thread(deleteThread);
                threadNum[threadIndex].Start(dm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            foreach (List<long> deleteRow in result)
            {
                cellIds.Remove(deleteRow);
            }
            long tableId = Database.getCurrentDatabase().getTableIdList()[Database.getCurrentDatabase().getTableNameList().IndexOf(TableName)];
            TableHeadCell thc = new TableHeadCell(this.TableName, this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
        }

        //  op  包括+ - * / =      其中 '= '表示直接赋值
        public void update(string fieldName, int flag, char op, object opNum, string con)
        {
            //计算线程数
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                UpdateMessage rm = new UpdateMessage(threadCount, threadIndex,
                    fieldName, flag, op, opNum, con);
                threadNum[threadIndex] = new Thread(UpdateFunction);
                threadNum[threadIndex].Start(rm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
        }
        private void UpdateFunction(Object Message)
        {
            UpdateMessage um = (UpdateMessage)Message;

            int start = -1;
            int end = -1;
            int c = cellIds.Count;
            int el = c / um.threadCount;
            if (um.threadCount != um.threadIndex + 1)
            {
                start = um.threadIndex * el;
                end = start + el - 1;
            }
            else
            {
                start = um.threadIndex * el;
                end = c - 1;
            }
            for (int i = start; i <= end; i++)
            {
                List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                    Global.CloudStorage.GetServerIdByCellId(cellIds[i][0]),
                    new GetRowMessageWriter(cellIds[i])).row;
                List<Object> values = FieldType.getValues(row, columnTypes);
                int index = this.columnNames.IndexOf(um.fieldname);
                int type = this.columnTypes[this.columnNames.IndexOf(um.fieldname)];
                int serverID;
                Condition con = new Condition(this, um.con);
                if (con.getResult(values))
                {
                    Element ele = new Element { };
                    using (var req = new GetElementMessageWriter(cellIds[i][index]))
                    {
                        serverID = Global.CloudStorage.GetServerIdByCellId(cellIds[i][index]);
                        using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID, req))
                        {
                            ele = responce.ele;
                        }
                    }
                    if (um.flag == 1)
                    {
                        if (type == 2)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.intField += (int)um.operationNum;
                                    break;
                                case '-':
                                    ele.intField -= (int)um.operationNum;
                                    break;
                                case '*':
                                    ele.intField *= (int)um.operationNum;
                                    break;
                                case '/':
                                    ele.intField /= (int)um.operationNum;
                                    break;
                                case '=':
                                    ele.intField = (int)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }
                        else if (type == 3)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.doubleField += (double)um.operationNum;
                                    break;
                                case '-':
                                    ele.doubleField -= (double)um.operationNum;
                                    break;
                                case '*':
                                    ele.doubleField *= (double)um.operationNum;
                                    break;
                                case '/':
                                    ele.doubleField /= (double)um.operationNum;
                                    break;
                                case '=':
                                    ele.doubleField = (double)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }
                        else if (type == 5)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.longField += (long)um.operationNum;
                                    break;
                                case '-':
                                    ele.longField -= (long)um.operationNum;
                                    break;
                                case '*':
                                    ele.longField *= (long)um.operationNum;
                                    break;
                                case '/':
                                    ele.longField /= (long)um.operationNum;
                                    break;
                                case '=':
                                    ele.longField = (long)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }

                    }
                    else
                    {
                        if (type == 2)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.intField += (int)um.operationNum;
                                    break;
                                case '-':
                                    ele.intField = (int)um.operationNum - ele.intField;
                                    break;
                                case '*':
                                    ele.intField *= (int)um.operationNum;
                                    break;
                                case '/':
                                    ele.intField = (int)um.operationNum / ele.intField;
                                    break;
                                case '=':
                                    ele.intField = (int)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }
                        else if (type == 3)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.doubleField += (double)um.operationNum;
                                    break;
                                case '-':
                                    ele.doubleField = (double)um.operationNum - ele.doubleField;
                                    break;
                                case '*':
                                    ele.doubleField *= (double)um.operationNum;
                                    break;
                                case '/':
                                    ele.doubleField = (double)um.operationNum / ele.doubleField;
                                    break;
                                case '=':
                                    ele.doubleField = (double)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }
                        else if (type == 5)
                        {
                            switch (um.op)
                            {
                                case '+':
                                    ele.longField += (long)um.operationNum;
                                    break;
                                case '-':
                                    ele.longField = (long)um.operationNum - ele.longField;
                                    break;
                                case '*':
                                    ele.longField *= (long)um.operationNum;
                                    break;
                                case '/':
                                    ele.longField = (long)um.operationNum / ele.longField;
                                    break;
                                case '=':
                                    ele.longField = (long)um.operationNum;
                                    break;
                                default:
                                    throw new Exception(String.Format("不合法的操作"));
                            }
                        }
                    }
                    ElementCell eleCell = FieldType.getElementCell(ele);
                    Global.CloudStorage.SaveElementCell(cellIds[i][index], eleCell);
                }
            }

        }
        public void update(string[] fieldNames, int val1, double val2, string con)
        {
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                UpdateFields uf = new UpdateFields(threadCount, threadIndex, fieldNames, val1, val2, con);
                threadNum[threadIndex] = new Thread(UpdateField);
                threadNum[threadIndex].Start(uf);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
        }
        private void UpdateField(Object Message)
        {
            UpdateFields uf = (UpdateFields)Message;
            int start = -1;
            int end = -1;
            int c = cellIds.Count;
            int el = c / uf.threadCount;
            if (uf.threadCount != uf.threadIndex + 1)
            {
                start = uf.threadIndex * el;
                end = start + el - 1;
            }
            else
            {
                start = uf.threadIndex * el;
                end = c - 1;
            }

            for (int i = start; i < end; i++)
            {
                List<Element> row = Global.CloudStorage.GetRowToDatabaseServer(
                Global.CloudStorage.GetServerIdByCellId(cellIds[i][0]),
                new GetRowMessageWriter(cellIds[i])).row;
                List<Object> values = FieldType.getValues(row, columnTypes);
                int index1 = this.columnNames.IndexOf(uf.fildNames[0]);
                int index2 = this.columnNames.IndexOf(uf.fildNames[1]);
                int serverID;
                Condition con = new Condition(this, uf.con);
                if (con.getResult(values))
                {
                    Element ele1 = new Element { };
                    using (var req = new GetElementMessageWriter(cellIds[i][index1]))
                    {
                        serverID = Global.CloudStorage.GetServerIdByCellId(cellIds[i][index1]);
                        using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID, req))
                        {
                            ele1 = responce.ele;
                            ele1.intField = uf.Value1;
                        }
                    }
                    Element ele2 = new Element { };
                    using (var req = new GetElementMessageWriter(cellIds[i][index2]))
                    {
                        serverID = Global.CloudStorage.GetServerIdByCellId(cellIds[i][index2]);
                        using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID, req))
                        {
                            ele2 = responce.ele;
                            ele2.doubleField = uf.Value2;
                        }
                    }
                    ElementCell eleCell1 = FieldType.getElementCell(ele1);
                    Global.CloudStorage.SaveElementCell(cellIds[i][index1], eleCell1);

                    ElementCell eleCell2 = FieldType.getElementCell(ele2);
                    Global.CloudStorage.SaveElementCell(cellIds[i][index2], eleCell1);
                }
            }
        }
        public void insert(string[] fieldNames, object[] values)
        {
            if (!isSingle)
            {
                throw new Exception(String.Format("不可输入多个表"));
            }
            List<long> ID = new List<long>();
            for (int i = 0; i < fieldNames.Length; i++)
            {
                ElementCell elecell = FieldType.setValueCell(values[i], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[i])));
                Global.CloudStorage.SaveElementCell(elecell);
                ID.Add(elecell.CellID);
            }
            this.cellIds.Add(ID);
            long tableId = Database.getCurrentDatabase().getTableIdList()[Database.getCurrentDatabase().getTableNameList().IndexOf(TableName)];
            TableHeadCell thc = new TableHeadCell(this.TableName, this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
        }
        private struct InsertMultiMessage
        {
            public int threadCount;
            public int threadIndex;
            public object[][] values;
            public ConcurrentBag<List<long>> result;
            public InsertMultiMessage(int threadCount, int threadIndex, object[][] values, ConcurrentBag<List<long>> result)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.values = values;
                this.result = result;
            }
        }
        private void insertMultiThread(Object insertMessage)
        {
            InsertMultiMessage imm = (InsertMultiMessage)insertMessage;
            int start = -1;
            int end = -1;
            int c = imm.values.Length;
            int el = c / imm.threadCount;
            if (imm.threadCount != imm.threadIndex + 1)
            {
                start = imm.threadIndex * el;
                end = start + el - 1;
            }
            else
            {
                start = imm.threadIndex * el;
                end = c - 1;
            }
            for (int i = start; i <= end; i++)
            {
                object[] values = imm.values[i];
                List<long> ID = new List<long>();
                for (int j = 0; j < values.Length; j++)
                {
                    ElementCell elecell = FieldType.setValueCell(values[j], this.columnTypes[j]);
                    Global.CloudStorage.SaveElementCell(elecell);
                    ID.Add(elecell.CellID);
                }
                imm.result.Add(ID);
            }
        }

        public void insert(string[] fieldNames, object[][] values)
        {
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            ConcurrentBag<List<long>> result = new ConcurrentBag<List<long>>();
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                InsertMultiMessage imm = new InsertMultiMessage(threadCount, threadIndex,
                    values, result);
                threadNum[threadIndex] = new Thread(insertMultiThread);
                threadNum[threadIndex].Start(imm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            cellIds.AddRange(result);
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(TableName));
            TableHeadCell thc = new TableHeadCell(this.TableName, this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);

        }

        private struct InsertTableMessage {
            public int threadCount;
            public int threadIndex;
            public Table anotherTable;
            public ConcurrentBag<List<long>> result;
            public InsertTableMessage(int threadCount, int threadIndex, Table anotherTable,
                ConcurrentBag<List<long>> result)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.anotherTable = anotherTable;
                this.result = result;
            }
        }

        private void insertTableThread(Object insertMessage)
        {
            InsertTableMessage imm = (InsertTableMessage)insertMessage;
            int start = -1;
            int end = -1;
            int c = imm.anotherTable.getCellIds().Count;
            int el = c / imm.threadCount;
            if (imm.threadCount != imm.threadIndex + 1)
            {
                start = imm.threadIndex * el;
                end = start + el - 1;
            }
            else
            {
                start = imm.threadIndex * el;
                end = c - 1;
            }
            for (int i = start; i <= end; i++)
            {
                List<Object> values = imm.anotherTable.getRow(i);
                List<long> ID = new List<long>();
                for (int j = 0; j < values.Count; j++)
                {
                    ElementCell elecell = FieldType.setValueCell(values[j], this.columnTypes[j]);
                    Global.CloudStorage.SaveElementCell(elecell);
                    ID.Add(elecell.CellID);
                }
                imm.result.Add(ID);
            }
        }
        public void insert(string[] fieldNames, Table anotherTable)
        {
            if (this.columnNames.Count != anotherTable.getColumnNames().Count) throw new Exception("无并相容性");

            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            ConcurrentBag<List<long>> result = new ConcurrentBag<List<long>>();
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                InsertTableMessage itm = new InsertTableMessage(threadCount, threadIndex,
                    anotherTable, result);
                threadNum[threadIndex] = new Thread(insertTableThread);
                threadNum[threadIndex].Start(itm);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            cellIds.AddRange(result);

            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(TableName));
            TableHeadCell thc = new TableHeadCell(this.TableName, this.columnNames, this.columnTypes, this.primaryIndexs, this.defaultValues, this.cellIds);
            Global.CloudStorage.SaveTableHeadCell(tableId, thc);
        }
        public void rename(string newName)
        {
            long tableId = Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(TableName
                  ));
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
        #endregion
        #region 邹开发 select

        /// <summary>
        /// 无into的select语句
        /// </summary>
        /// <param name="fields">三元组数组，第一个表示字段，第二个表示别名</param>
        /// <param name="con">条件表达式</param>
        /// <returns></returns>
        public Table select(Tuple<string, string>[] fields, string con, List<int> nestedColumn = null)
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
                for(int i = 0; i < columnNames.Count; i++)
                {
                    usedIndexes.Add(i);
                }
            }
            else
            {
                foreach (Tuple<string, string> field in fields)
                {
                    string fieldName = field.Item1;  //字段名
                    if (columnNames.Contains(fieldName)
                        )  //直接能识别出该字段
                    {
                        newColumnNames.Add((field.Item2 == null) || (field.Item2.Equals("")) ?
                            fieldName : field.Item2);  //别名
                        newColumnTypes.Add(columnTypes[columnNames.IndexOf(fieldName)]);
                        usedIndexes.Add(columnNames.IndexOf(fieldName));
                    } else if(columnNames.Contains(fieldName.Substring(fieldName.IndexOf(".") + 1)))
                    {
                        fieldName = fieldName.Substring(fieldName.IndexOf(".") + 1);
                        newColumnNames.Add((field.Item2 == null) || (field.Item2.Equals("")) ?
                            fieldName : field.Item2);  //别名
                        newColumnTypes.Add(columnTypes[columnNames.IndexOf(fieldName)]);
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
                            newColumnTypes.Add(fieldIndex);
                            usedIndexes.Add(fieldIndex);
                        }
                    }
                }
            }
            SelectMessageWriter smw = new SelectMessageWriter(columnNames, columnTypes, cellIds, usedIndexes, con,
                nestedColumn);
            //此时，新表的结构构建完成，向proxy发送查询任务
            List<List<long>> newCellIds = Global.CloudStorage.SelectFromClientToDatabaseProxy(0, smw).cellIds;
            return new Table(newCellIds, newColumnTypes, newColumnNames, this.primaryIndexs,
                this.defaultValues, this.TableName);
        }

        public Table select(List<int> usedInd)
        {
            //新表的结构
            List<string> newColumnNames = new List<string>();
            List<int> newColumnTypes = new List<int>();
            for(int i = 0; i<usedInd.Count; i++)
            {
                newColumnNames.Add(columnNames[usedInd[i]]);
                newColumnTypes.Add(columnTypes[usedInd[i]]);
            }
            SelectMessageWriter smw = new SelectMessageWriter(columnNames, columnTypes, cellIds, usedInd, null);
            List<List<long>> newCellIds = Global.CloudStorage.SelectFromClientToDatabaseProxy(0, smw).cellIds;
            return new Table(newCellIds, newColumnTypes, newColumnNames, this.primaryIndexs,
                this.defaultValues, this.TableName);
        }

        #endregion
        #region 李宁 join topk union

        /// <summary>
        /// distinct 多线程可行
        /// </summary>
        /// <param name="correspond">排序后的,count必须大于等于2</param>
        private List<dint> distinct(List<List<Element>> correspond)
        {
            if (correspond.Count < 2) throw new Exception("count必须大于等于2");
            List<dint> res = new List<dint>();
            int s = 0;
            int e = 0;
            List<Element> ele = correspond[0];
            for (int i = 1; i < correspond.Count + 1; i++)
            {
                if (i < correspond.Count && Equal(ele, correspond[i]) > 0)
                {
                    e++;
                }
                else
                {
                    for (int j = s; j <= e; j++)
                        res.Add(new dint(s, e));
                    s = i;
                    if (i < correspond.Count) ele = correspond[s];
                    e = i;
                }
            }
            return res;
        }
        /// <summary>
        /// union distinct 假设重复是由两表合并引起的
        /// </summary>
        /// <param name="anotherTable"></param>
        /// <returns>this</returns>
        public Table union_distinct(Table anotherTable)
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                if (!columnNames[i].Equals(anotherTable.columnNames[i])) throw new Exception("两表无并相容性");
            }
            if (anotherTable.cellIds.Count == 0) throw new Exception("anotherTable不能为空");
            UnionMessageWriter msg = new UnionMessageWriter(this.cellIds, anotherTable.cellIds);
            List<List<long>> temp =  Global.CloudStorage.UnionFromClientToDatabaseProxy(0, msg).cellids;
            this.cellIds.AddRange(temp);
            return this;
        }
        /// <summary>
        /// union all
        /// </summary>
        /// <param name="anotherTable"></param>
        /// <returns>并不是生成新表而是把anothertable 加入到this中</returns>
        public Table union_all(Table anotherTable)
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                if (!columnNames[i].Equals(anotherTable.columnNames[i])) throw new Exception("两表无并相容性");
            }
            cellIds.AddRange(anotherTable.cellIds);
            return this;
        }
        /// <summary>
        /// 将列名转化为位置序号
        /// </summary>
        private int nametopos(string name)
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                if (columnNames[i].Equals(name))
                {
                    return i;
                }
            }
            throw new Exception("不存在的列名");
        }
        /// <summary>
        /// topk 目前的names.count = 1 主要在服务器端未实现多值比较
        /// </summary>
        public List<List<long>> topK(int k, string[] names)
        {
            if (k==0) throw new Exception("k不能为0");
            List<int> pos = new List<int>();
            foreach (var name in names)
            {
                pos.Add(nametopos(name));
            }
            TopKMessageWriter msg = new TopKMessageWriter(k, pos, this.cellIds);
            List<List<long>> r = Global.CloudStorage.TopKFromClientToDatabaseProxy(0, msg).celllids;
            return r;
        }
        public List<List<long>> topKOnLocal(int k, string name)
        {
            if (k == 0) throw new Exception("k不能为0");
            int pos = nametopos(name);
            List<int> cond = new List<int>();
            cond.Add(pos);
            List<List<Element>> correspondA = getCorrespon(cond, this.cellIds);

            List<List<long>> res = new List<List<long>>(k);
            List<int> cmp = new List<int>(k);
            for (int i = 0; i < k; i++)
            {
                cmp.Add(-1);
                res.Add(null);
            }
            for (int a = 0; a < correspondA.Count; a++)
            {
                bool flag = false;
                int p = 0;
                while (p < k && correspondA[a][0].intField > cmp[p])
                {
                    ++p;
                    flag = true;
                }
                if (flag)
                {
                    int tint = correspondA[a][0].intField;
                    List<long> tres = cellIds[a];
                    while (p - 1 >= 0)
                    {
                        int tint2 = cmp[p - 1];
                        List<long> tres2 = res[p - 1];
                        cmp[p - 1] = tint;
                        res[p - 1] = tres;
                        tint = tint2;
                        tres = tres2;
                        --p;
                    }
                }
            }
            for (int i = 0; i < (k) / 2; i++)
            {
                int tint = cmp[i];
                List<long> tres = res[i];
                cmp[i] = cmp[k - i - 1];
                res[i] = res[k - i - 1];
                cmp[k - i - 1] = tint;
                res[k - i - 1] = tres;
            }
            return res;
        }
        private List<dint> calcond(List<string> another)
        {
            List<dint> res = new List<dint>();
            for (var a = 0; a < this.columnNames.Count; a++)
            {
                for (var b = 0; b < another.Count; b++)
                {
                    if (columnNames[a].Equals(another[b]))
                    {
                        res.Add(new dint(a, b));
                        continue;
                    }
                }
            }
            return res;
        }
        /// <summary>
        /// 获取几列内容 get row
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="control"></param>
        /// <param name="another">if control is 1 another can't be null</param>
        public static List<List<Element>> getCorrespon(List<int> ids, List<List<long>> ID)
        {
            var res = new List<List<Element>>();
            List<long> temp = new List<long>();

            foreach (var e in ID)
            {
                foreach (int id in ids)
                {
                    temp.Add(e[id]);
                }
                GetRowMessageWriter msg = new GetRowMessageWriter(temp);
                GetRowResponseReader r = Global.CloudStorage.GetRowToDatabaseServer(0, msg);
                res.Add(r.row);
                temp.Clear();
            }
            return res;
        }
        /// <summary>
        /// 自定义快排,多线程待实现 left =0; right = count-1
        /// </summary>
        /// <param name="control">默认为inc 若为-1 则dec</param>
        public static void QuickSort(List<List<Element>> array, int left, int right, List<List<long>> t, int control = 1)
        {

            if (left < right)
            {

                int middle = GetMiddleFroQuickSort(array, left, right, t, control);

                QuickSort(array, left, middle - 1, t);

                QuickSort(array, middle + 1, right, t);
            }

        }
        public static void QuickSort_multithread(List<List<Element>> array, int left, int right, List<List<long>> t, int control = 1)
        {
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            if (left < right)
            {

                int middle = GetMiddleFroQuickSort(array, left, right, t, control);

                QuickSort_multithread(array, left, middle - 1, t);

                QuickSort_multithread(array, middle + 1, right, t);
            }
        }
        private static int GetMiddleFroQuickSort(List<List<Element>> array, int left, int right, List<List<long>> t, int control = 1)
        {
            List<Element> key = array[left];
            List<long> ktemp = t[left];
            while (left < right)
            {
                while (left < right && CopTo(key, array[right], control) < 0)
                {
                    right--;
                }
                if (left < right)
                {
                    List<Element> temp = array[left];
                    array[left] = array[right];

                    List<long> tempp = t[left];
                    t[left] = t[right];
                    left++;
                }

                while (left < right && CopTo(key, array[left], control) > 0)
                {
                    left++;
                }
                if (left < right)
                {
                    List<Element> temp = array[right];
                    array[right] = array[left];

                    List<long> tempp = t[right];
                    t[right] = t[left];
                    right--;
                }
                array[left] = key;
                t[left] = ktemp;
            }
            return left;
        }
        /// <summary>
        /// 比较函数compare to
        /// </summary>
        /// <param name="control">默认为inc 若为-1 则dec</param>
        private static int CopTo(List<Element> key, List<Element> arr, int control = 1)
        {
            for (int i = 0; i < key.Count; i++)
            {
                if (key[i].intField < arr[i].intField)
                    return -control;
                if (key[i].intField > arr[i].intField)
                    return control;
            }
            return control;
        }
        struct JoinThreadObject
        {
            public int threadCount;
            public int threadIndex;
            public Table another;
            public Table newtable;
            public List<List<List<long>>> res;
            public JoinThreadObject(int threadCount, int threadIndex, Table another, Table newtable, List<List<List<long>>> res)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.newtable = newtable;
                this.another = another;
                this.res = res;
            }
        }
        void JoinThreadProc(object par)
        {
            JoinThreadObject p = (JoinThreadObject)par;
            int start = -1;
            int end = -1;
            Table another = p.another;
            Table newtable = p.newtable;
            int c = this.cellIds.Count;
            int ele = c / p.threadCount;
            if (p.threadCount != p.threadIndex + 1)
            {
                start = p.threadIndex * ele;
                end = start + ele - 1;
            }
            else
            {
                start = p.threadIndex * ele;
                end = c - 1;
            }


            for (int i = start; i <= end; i++)
            {
                foreach (var a in another.cellIds)
                {
                    List<long> row = new List<long>();
                    row.AddRange(cellIds[i]);
                    row.AddRange(a);
                    p.res[p.threadIndex].Add(row);
                }
            }
        }
        /// <summary>
        /// equal only intfield
        /// </summary>
        /// <returns></returns>
        private static int Equal(List<Element> A, List<Element> B)
        {
            for (int i = 0; i < A.Count; i++)
            {
                if (A[i].intField != B[i].intField)
                    return -1;
            }
            return 1;
        }
        public static int BinSearch(List<List<Element>> correspondA, List<Element> key, int low)
        {
            int array_size = correspondA.Count;
            int high = array_size - 1, mid;

            while (low <= high)
            {
                mid = (low + high) / 2;//获取中间的位置  

                if (Equal(correspondA[mid], key) > 0)
                    return mid; //找到则返回相应的位置  
                if (CopTo(correspondA[mid], key) > 0)
                    high = mid - 1; //如果比key大，则往低的位置查找  
                else
                    low = mid + 1;  //如果比key小，则往高的位置查找  
            }
            return -1;
        }
        struct JoinJudgeThreadObject
        {
            public int threadCount;
            public int threadIndex;
            public Table another;
            public Table newtable;
            public List<List<Element>> correspondA;
            public List<List<Element>> correspondB;
            public List<dint> range;
            public List<List<List<long>>> res;
            public JoinJudgeThreadObject(int threadCount, int threadIndex, Table another, Table newtable,
                List<List<Element>> correspondA, List<List<Element>> correspondB, List<dint> range, List<List<List<long>>> res)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.newtable = newtable;
                this.another = another;
                this.correspondA = correspondA;
                this.correspondB = correspondB;
                this.range = range;
                this.res = res;
            }
        }
        void JoinJudgeThreadProc(object par)
        {
            JoinJudgeThreadObject p = (JoinJudgeThreadObject)par;
            int start = -1;
            int end = -1;
            Table another = p.another;
            Table newtable = p.newtable;
            List<List<Element>> correspondA = p.correspondA;
            List<List<Element>> correspondB = p.correspondB;
            List<dint> range = p.range;
            List<List<long>> res = p.res[p.threadIndex];

            int c = another.cellIds.Count;
            int ele = c / p.threadCount;

            if (p.threadCount != p.threadIndex + 1)
            {
                start = p.threadIndex * ele;
                end = start + ele - 1;
            }
            else
            {
                start = p.threadIndex * ele;
                end = c - 1;
            }
            int i = start;
            int low = 0;
            while (i <= end)
            {
                int s, e;
                s = i;
                e = i;
                while (e <= end - 1 && Equal(correspondB[s], correspondB[e + 1]) > 0)
                {
                    e++;
                }
                int pos = BinSearch(correspondA, correspondB[i], low);
                if (pos == -1)//no match
                {
                    i = e + 1;
                    continue;
                }
                else//match
                {
                    int s1, e1;
                    s1 = range[pos].a;
                    e1 = range[pos].b;
                    for (int j = s1; j <= e1; j++)//this
                    {
                        for (int ii = s; ii <= e; ii++) //another
                        {
                            List<long> row = new List<long>();
                            row.AddRange(this.cellIds[j]);
                            row.AddRange(another.cellIds[ii]);
                            res.Add(row);
                        }
                    }
                    i = e + 1;
                    low = e1 + 1;
                }
            }
        }
        public Table innerJoin(Table anotherTable, String newTableName = null, List<dint> cond = null, bool isLocal = true)
        {
            //first 
            //cellids columnNames columntypes
            Table newtable = new Table();
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            List<List<List<long>>> res = new List<List<List<long>>>(threadCount);
            for (int a = 0; a < threadCount; a++)
            {
                res.Add(new List<List<long>>());
            }
            
            if (isLocal)
            {
                if (newTableName == null) throw new Exception("newTableName can't be null");
                newtable.TableName = newTableName;

                foreach (var a in this.columnNames)
                    newtable.columnNames.Add(TableName + "." + a);
                foreach (var a in this.columnTypes)
                    newtable.columnTypes.Add(a);
                foreach (var a in anotherTable.columnNames)
                    newtable.columnNames.Add(anotherTable.TableName + "." + a);
                foreach (var a in anotherTable.columnTypes)
                    newtable.columnTypes.Add(a);
            }
            
            //process
            if (cond == null)//使用默认条件，名字相同
                cond = calcond(anotherTable.columnNames);
            if (cond.Count != 0)//使用自定义条件
            {
                List<int> conda = new List<int>();
                List<int> condb = new List<int>();
                foreach (var a in cond)
                {
                    conda.Add(a.a);
                    condb.Add(a.b);
                }
                List<List<Element>> correspondA = getCorrespon(conda, this.cellIds);
                List<List<Element>> correspondB = getCorrespon(condb, anotherTable.cellIds);

                QuickSort(correspondA, 0, correspondA.Count - 1, this.cellIds);
                QuickSort(correspondB, 0, correspondB.Count - 1, anotherTable.cellIds);

                List<dint> range = distinct(correspondA);//get the range
                for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
                {
                    JoinJudgeThreadObject p = new JoinJudgeThreadObject(threadCount, threadIndex, anotherTable, newtable, correspondA, correspondB, range, res);
                    threadNum[threadIndex] = new Thread(JoinJudgeThreadProc);
                    threadNum[threadIndex].Start(p);
                }
                for (int inde = 0; inde < threadCount; inde++)
                {
                    threadNum[inde].Join();
                }
                for (int inde = 0; inde < threadCount; inde++)
                {
                    newtable.cellIds.AddRange(res[inde]);
                }
            }
            else//使用恒true条件
            {
                for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
                {
                    JoinThreadObject p = new JoinThreadObject(threadCount, threadIndex, anotherTable, newtable, res);
                    threadNum[threadIndex] = new Thread(JoinThreadProc);
                    threadNum[threadIndex].Start(p);
                }
                for (int inde = 0; inde < threadCount; inde++)
                    threadNum[inde].Join();
                for (int inde = 0; inde < threadCount; inde++)
                    newtable.cellIds.AddRange(res[inde]);
            }
            return newtable;
        }
        /// <summary>
        /// 按照another第一个元素进行分类 another尽可能是单表, this 最好尺寸小
        /// </summary>
        /// <param name="anotherTable"></param>
        /// <param name="cond">条件表达式</param>
        /// <returns></returns>
        public Table innerJoinOnCluster(Table anotherTable, String TableName, List<dint> cond = null)
        {
            //first 
            //cellids columnNames columntypes
            Table newtable = new Table();
            newtable.TableName = TableName;
            foreach (var a in this.columnNames)
                newtable.columnNames.Add(this.TableName + "." + a);
            foreach (var a in this.columnTypes)
                newtable.columnTypes.Add(a);
            foreach (var a in anotherTable.columnNames)
                newtable.columnNames.Add(anotherTable.TableName + "." + a);
            foreach (var a in anotherTable.columnTypes)
                newtable.columnTypes.Add(a);
            //process
            if (cond == null)//使用默认条件，名字相同
                cond = calcond(anotherTable.columnNames);

            List<int> conda = new List<int>();
            List<int> condb = new List<int>();
            foreach (var a in cond)
            {
                conda.Add(a.a);
                condb.Add(a.b);
            }
            JoinMessageWriter msg = new JoinMessageWriter(this.cellIds, anotherTable.cellIds, conda, condb);
            newtable.cellIds = Global.CloudStorage.JoinFromClientToDatabaseProxy(0, msg).celllids;

            return newtable;
        }
        #endregion
        #region getRow, getColumn, print


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

        public List<int> getPrimayIndexs()
        {
            return primaryIndexs;
        }
        public string[] getColumnNamesString()
        {
            return this.getColumnNames().ToArray();
        }
        public string getTableName() {
			return TableName;
		}
        public void setTableName(string tablename)
        {
           this.TableName = tablename;
        }
        public List<Element> getDefaultValues()
        {
            return defaultValues;
        }

        public void printTable()
        {
            foreach (string name in columnNames)
            {
                Console.Write("{0, -15}", name);
            }
            Console.WriteLine();
            for (int i = 0; i < cellIds.Count; i++)
            {
                List<Object> row = getRow(i);
                foreach (Object ele in row)
                {
                    Console.Write("{0, -15}", ele);
                }
                Console.WriteLine();
            }
        }
        #endregion

        public static int elementIndex(List<List<long>> cells, List<long> tempList)
        {
            int index = -1;
            for (int i = 0; i < cells.Count; i++)
            {
                int flag = 1;
                for (int j = 0; j < tempList.Count; j++)
                {
                    Element ele1 = new Element();
                    Element ele2 = new Element();
                    using (var req1 = new GetElementMessageWriter(cells[i][j]))
                    {
                        int serverID1 = Global.CloudStorage.GetServerIdByCellId(cells[i][j]);
                        using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID1, req1))
                        {
                            ele1 = responce.ele;
                        }
                    }
                    using (var req2 = new GetElementMessageWriter(tempList[j]))
                    {
                        int serverID2 = Global.CloudStorage.GetServerIdByCellId(tempList[j]);
                        using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID2, req2))
                        {
                            ele2 = responce.ele;
                        }
                    }
                    if (!ele1.Equals(ele2))
                    {
                        flag = 0;
                        break;
                    }
                }
                if (flag == 1)
                {
                    index = i;
                    break;
                }

            }
            return index;
        }

        public static void printEle(List<List<long>> cells)
        {
            for (int i = 0; i < cells.Count; i++)
            {
                Element ele1 = new Element();
                Element ele2 = new Element();
                using (var req1 = new GetElementMessageWriter(cells[i][0]))
                {
                    int serverID1 = Global.CloudStorage.GetServerIdByCellId(cells[i][0]);
                    using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID1, req1))
                    {
                        ele1 = responce.ele;
                    }
                }
                using (var req2 = new GetElementMessageWriter(cells[i][1]))
                {
                    int serverID2 = Global.CloudStorage.GetServerIdByCellId(cells[i][1]);
                    using (var responce = Global.CloudStorage.GetElementToDatabaseServer(serverID2, req2))
                    {
                        ele2 = responce.ele;
                    }
                }
                Console.WriteLine("{0}\t{1}", ele1.intField, ele2.intField);
            }
        }

    }

}