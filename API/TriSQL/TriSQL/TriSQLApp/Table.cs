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
        #region 字段
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
        #endregion
        #region 构造
        public Table(List<List<long>> cellIds)
        {
            this.cellIds = cellIds;
        }
        public Table(List<List<long>> cellIds, List<int> columnTypes)
        {
            this.cellIds = cellIds;
            this.columnTypes = columnTypes;
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
                this.tableNames.Add(tableName);
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
                    }
                }
        }
        public Table MultiTable(params string[] tableName)
        {
            Table M;
            Table t1 = new Table(tableName[0]);
            Table t2 = new Table(tableName[1]);
            M = t1.innerJoinOnCluster(t2, new List<dint>());
            for (int i = 2; i < tableName.Length; i++)
            {
                Table temp = new Table(tableName[i]);
                 M = M.innerJoinOnCluster(temp,new List<dint>());
            }
            return M;
        }
        #endregion
        #region 田超-delete,update,insert,truncate,rename
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
            //Condition contemp = new Condition(table, con);
            List<Thread> threads = new List<Thread> { };
            foreach (List<long> Id in this.cellIds)
            {
                UpdateMessage um = new UpdateMessage();
                //um.con = contemp;
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
            //if (um.con.getResult(values))//um.con.getResult(values)
            if (true)
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
            List<long> ID = new List<long>();
            ElementCell elecell = FieldType.setValueCell(values[0], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[0])));
            Global.CloudStorage.SaveElementCell(elecell.CellID, elecell);
            ID.Add(elecell.CellID);
            for (int i = 1; i < fieldNames.Length; i++)
            {
                Element temp = FieldType.setValue(values[i], this.columnTypes.ElementAt(columnNames.IndexOf(fieldNames[i])));
                ele.Add(temp);
            }
            List<long> ids = Global.CloudStorage.InsertElementToDatabaseServer(0,
                new InsertMessageWriter(ele)).cellIds;
            for (int k = 0; k < ids.Count; k++)
            {
                ID.Add(ids[k]);
            }
            this.cellIds.Add(ID);
            long tableId = Database.getCurrentDatabase().getTableIdList()[Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                 [0])];
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
        #endregion
        #region 邹开发 select

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
            usedIndexes.Sort();
            //此时，新表的结构构建完成，向proxy发送查询任务
            SelectMessageWriter smw = new SelectMessageWriter(columnNames, columnTypes, cellIds, usedIndexes, con);
            List<List<long>> newCellIds = Global.CloudStorage.SelectFromClientToDatabaseProxy(0, smw).cellIds;
            return new Table(newCellIds, newColumnTypes, newColumnNames, this.primaryIndexs,
                this.defaultValues, this.tableNames);
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
                    e = i;
                }
            }
            return res;
        }
        /// <summary>
        /// union distinct 猜测效率很低
        /// </summary>
        /// <param name="anotherTable"></param>
        public Table union_distinct(Table anotherTable)
        {
            for (int i = 0; i < columnNames.Count; i++)
            {
                if (!columnNames[i].Equals(anotherTable.columnNames)) throw new Exception("两表无并相容性");
            }
            cellIds.AddRange(anotherTable.cellIds);


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
                if (!columnNames[i].Equals(anotherTable.columnNames)) throw new Exception("两表无并相容性");
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
            public JoinThreadObject(int threadCount, int threadIndex, Table another, Table newtable)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.newtable = newtable;
                this.another = another;
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
                    newtable.cellIds.Add(row);
                }
            }

        }
        /// <summary>
        /// equal only intfield
        /// </summary>
        /// <returns></returns>
        private int Equal(List<Element> A, List<Element> B)
        {
            for (int i = 0; i < A.Count; i++)
            {
                if (A[i].intField != B[i].intField)
                    return -1;
            }
            return 1;
        }
        int BinSearch(List<List<Element>> correspondA, List<Element> key)
        {
            int array_size = correspondA.Count;
            int low = 0, high = array_size - 1, mid;

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
            public JoinJudgeThreadObject(int threadCount, int threadIndex, Table another, Table newtable,
                List<List<Element>> correspondA, List<List<Element>> correspondB, List<dint> range)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.newtable = newtable;
                this.another = another;
                this.correspondA = correspondA;
                this.correspondB = correspondB;
                this.range = range;
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
            while (i <= end)
            {
                int s, e;
                s = i;
                e = i;
                while (e <= end - 1 && Equal(correspondB[s], correspondB[e + 1]) > 0)
                {
                    e++;
                }
                int pos = BinSearch(correspondA, correspondB[i]);
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
                            newtable.cellIds.Add(row);
                        }
                    }
                    i = e + 1;
                }
            }
        }
        public Table innerJoin(Table anotherTable, List<dint> cond = null, bool isLocal = true)
        {
            //first 
            //cellids columnNames columntypes
            Table newtable = new Table();
            if (isLocal)
            {
                foreach (var a in this.columnNames)
                    newtable.columnNames.Add(tableNames[0] + "." + a);
                foreach (var a in this.columnTypes)
                    newtable.columnTypes.Add(a);
                foreach (var a in anotherTable.columnNames)
                    newtable.columnNames.Add(anotherTable.tableNames[0] + "." + a);
                foreach (var a in anotherTable.columnTypes)
                    newtable.columnTypes.Add(a);
                newtable.tableNames.Add(this.tableNames[0] + anotherTable.tableNames[0]);
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

                int threadCount = Environment.ProcessorCount;
                Thread[] threadNum = new Thread[threadCount];
                List<dint> range = distinct(correspondA);//get the range
                for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
                {
                    JoinJudgeThreadObject p = new JoinJudgeThreadObject(threadCount, threadIndex, anotherTable, newtable, correspondA, correspondB, range);
                    threadNum[threadIndex] = new Thread(JoinJudgeThreadProc);
                    threadNum[threadIndex].Start(p);
                }
                for (int inde = 0; inde < threadCount; inde++)
                    threadNum[inde].Join();
            }
            else//使用恒true条件
            {
                int threadCount = Environment.ProcessorCount;
                Thread[] threadNum = new Thread[threadCount];

                for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
                {
                    JoinThreadObject p = new JoinThreadObject(threadCount, threadIndex, anotherTable, newtable);
                    threadNum[threadIndex] = new Thread(JoinThreadProc);
                    threadNum[threadIndex].Start(p);
                }
                for (int inde = 0; inde < threadCount; inde++)
                    threadNum[inde].Join();
            }
            return newtable;
        }
        /// <summary>
        /// 按照another第一个元素进行分类 another尽可能是单表, this 最好尺寸小
        /// </summary>
        /// <param name="anotherTable"></param>
        /// <param name="cond">条件表达式</param>
        /// <returns></returns>
        public Table innerJoinOnCluster(Table anotherTable, List<dint> cond = null)
        {
            //first 
            //cellids columnNames columntypes
            Table newtable = new Table();
            foreach (var a in this.columnNames)
                newtable.columnNames.Add(tableNames[0] + "." + a);
            foreach (var a in this.columnTypes)
                newtable.columnTypes.Add(a);
            foreach (var a in anotherTable.columnNames)
                newtable.columnNames.Add(anotherTable.tableNames[0] + "." + a);
            foreach (var a in anotherTable.columnTypes)
                newtable.columnTypes.Add(a);
            newtable.tableNames.Add(this.tableNames[0] + anotherTable.tableNames[0]);
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
		
		public List<string> getTableNames() {
			return tableNames;
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
    }

}