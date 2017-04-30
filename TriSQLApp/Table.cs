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
        private List<List<long>> cellIds;
        private List<int> columnTypes;
        private List<string> columnNames;
        private List<int> primaryIndexs;  //主键索引
        private List<Element> defaultValues;  //默认值
        private string tablename;
        private List<string> tableNames = new List<string> { };
        public Table()
        {
            cellIds = new List<List<long>>();
            columnNames = new List<string>();
            columnTypes = new List<int>();
            primaryIndexs = new List<int>();
            defaultValues = new List<Element>();
            
        }
        
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
                List<int> countNum = new List<int> { };
                List<int> Num = new List<int> { };
                List<List<List<long>>> CID = new List<List<List<long>>> { };
                isSingle = false;

                for (int i = 0; i < tableName.Length; i++)
                {
                    if (!Database.getCurrentDatabase().tableExists(tableName[i]))
                    {
                        throw new Exception(String.Format("当前表{0}不存在!", tableName[i]));
                    }
                    this.tableNames.Add(tableName[0]);
                    tableIds.Add(Database.getCurrentDatabase().getTableIdList().ElementAt(Database.getCurrentDatabase().getTableNameList().IndexOf(tableNames
                   [i])));
                    using (var request = new GetTableMessageWriter(tableIds[i]))
                    {
                        int serverId = Global.CloudStorage.GetServerIdByCellId(tableIds[i]);
                        using (var res = Global.CloudStorage.GetTableToDatabaseServer(serverId, request))
                        {
                            for (int j = 0; j < res.columnNameList.Count; j++)
                            {
                                this.columnNames.Add(res.tableName + '.' + res.columnNameList[j]);
                                this.columnTypes.Add(res.columnTypeList[j]);
                            }
                            countNum.Add(res.cellIds.Count);
                            CID.Add(res.cellIds);
                        }
                    }
                }
                int sum = 1;
                for (int i = 0; i < tableName.Length; i++)
                {
                    sum = sum * countNum[i];

                }
                for (int j = 0; j < tableName.Length; j++)
                {
                    int tempNum = 1;
                    for (int i = tableName.Length - 1; i > j; i--)
                    {
                        tempNum = tempNum * countNum[i];
                    }
                    Num.Add(tempNum);
                }
                //  Console.WriteLine("{0}", sum);
                List<long> temp = new List<long> { };
                for (int i = 0; i < tableName.Length; i++)
                {
                    List<List<long>> tempList = new List<List<long>> { };
                    for (int j = 0; j < sum / (Num[i] * countNum[i]); j++)
                    {
                        for (int k = 0; k < countNum[i]; k++)
                        {
                            for (int l = 0; l < Num[i]; l++)
                            {
                                tempList.Add(CID[i][k]);
                            }
                        }
                    }
                    // this.cellIds.Add(tempList);
                }


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
            if (true)//um.con.getResult(values)
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
                            break;
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
                            break;
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

        public List<List<long>> getCellIds()
        {
            return this.cellIds;
        }



/*
 
             李宁 
             --------------------------------------------------------------------------------------------------------------
             */








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
            for(int i = 1; i<correspond.Count; i++)
            {
                if (Equal(ele, correspond[i]) > 0)
                {
                    e++;
                }else
                {
                    for (int j = s; j<=e; j++)
                        res.Add(new dint(s, e));
                    s = i;
                    e = i;
                }
            }
            return res;
        }
        /// <summary>
        /// union distinct
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
        public Table union_all(Table anotherTable)
        {
            for(int i = 0; i< columnNames.Count; i++)
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
        /// topk
        /// </summary>
        public List<List<long>> topK(int k, string name)
        {
            int pos = nametopos(name);
            List<dint> cond = new List<dint>();
            cond.Add(new dint(pos, 0));
            List<List<Element>> correspondA = getCorrespon(cond, 0);

            List<List<long>> res = new List<List<long>>(k);
            int[] cmp = new int[k];

            for (int a = 0; a< correspondA.Count; a++)
            {
                bool flag = false;
                int p = 0;
                while (correspondA[a][0].intField > cmp[p] && p < k )
                {
                    ++p;
                    flag = true;
                }
                if (flag)
                {
                    cmp[p - 1] = correspondA[a][0].intField;
                    res[p - 1] = cellIds[a];
                }
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
        private List<List<Element>> getCorrespon(List<dint> ids, int control, Table another = null)
        {
            var res = new List<List<Element>>();
            List<long> temp = new List<long>();
            List<List<long>> ID = null;
            if (control == 0)
                ID = this.cellIds;
            else
            {
                if (another == null) throw new Exception("another can't be null");
                ID = another.cellIds;
            }

            foreach (var e in ID)
            {
                foreach (dint id in ids)
                {
                    if (control == 0)
                        temp.Add(e[id.a]);
                    else
                        temp.Add(e[id.b]);
                }
                GetRowMessageWriter msg = new GetRowMessageWriter(temp);
                GetRowResponseReader r = Global.CloudStorage.GetRowToDatabaseServer(0,msg);
                res.Add(r.row);
                temp.Clear();
            }
            return res;
        }
        /// <summary>
        /// 自定义快排,多线程待实现
        /// </summary>
        /// <param name="control">默认为inc 若为-1 则dec</param>
        public static void QuickSort(List<List<Element>> array, int left, int right, Table t, int control = 1)
        {

            if (left < right)
            {

                int middle = GetMiddleFroQuickSort(array, left, right, t, control);

                QuickSort(array, left, middle - 1, t);

                QuickSort(array, middle + 1, right, t);
            }

        }
        public static void QuickSort_multithread(List<List<Element>> array, int left, int right, Table t, int control = 1)
        {
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            if (left < right)
            {

                int middle = GetMiddleFroQuickSort(array, left, right, t, control);

                QuickSort(array, left, middle - 1, t);

                QuickSort(array, middle + 1, right, t);
            }
        }
        private static int GetMiddleFroQuickSort(List<List<Element>> array, int left, int right, Table t, int control = 1)
        {
            List<Element> key = array[left];
            while (left < right)
            {
                while (left < right && CopTo(key,array[right], control) < 0)
                {
                    right--;
                }
                if (left < right)
                {
                    List<Element> temp = array[left];
                    array[left] = array[right];

                    List<long> tempp = t.cellIds[left];
                    t.cellIds[left] = t.cellIds[right];
                    left++;
                }

                while (left < right && CopTo(key,array[left], control) > 0)
                {
                    left++;
                }
                if (left < right)
                {
                    List<Element> temp = array[right];
                    array[right] = array[left];

                    List<long> tempp = t.cellIds[right];
                    t.cellIds[right] = t.cellIds[left];
                    right--;
                }
                array[left] = key;
            }
            return left;
        }
        /// <summary>
        /// 比较函数compare to
        /// </summary>
        /// <param name="control">默认为inc 若为-1 则dec</param>
        private static int CopTo(List<Element> key, List<Element> arr, int control = 1)
        {
            for (int i = 0; i< key.Count; i++)
            {
                if (key[i].intField < arr[i].intField)
                    return -control;
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
            int c = this.cellIds.Count;
            int start = -1;
            int end = -1;
            int ele = c / p.threadCount;
            Table another = p.another;
            Table newtable = p.newtable;
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
            
            
            for (int i = start; i< end; i++)
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
        int Equal(List<Element> A, List<Element> B)
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
                if (CopTo(correspondA[mid], key) < 0) 
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
            int c = this.cellIds.Count;
            int start = -1;
            int end = -1;
            int ele = c / p.threadCount;
            Table another = p.another;
            Table newtable = p.newtable;
            List<List<Element>> correspondA = p.correspondA;
            List<List<Element>> correspondB = p.correspondB;
            List<dint> range = p.range;

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
            while (i < end)
            {
                int s, e;
                s = i;
                e = i;
                while (Equal(correspondB[s], correspondB[e + 1]) > 1)
                {
                    e++;
                }
                int pos = BinSearch(correspondA, correspondB[i]);//this
                if (pos == -1)
                {
                    i = e;
                    continue;
                }else
                {
                    int s1, e1;
                    s1 = range[pos].a;
                    e1 = range[pos].b;
                    for (int j = s1; j < e1; j++)//this
                    {
                        for (int ii = s; ii < e; ii++) //another
                        {
                            List<long> row = new List<long>();
                            row.AddRange(this.cellIds[j]);
                            row.AddRange(another.cellIds[ii]);
                            newtable.cellIds.Add(row);
                        }
                    }
                }
            }
        }
        public Table innerJoin(Table anotherTable, List<dint> cond = null)
        {
            //first 
            //cellids columnNames columntypes
            Table newtable = new Table();
            foreach (var a in this.columnNames)
                newtable.columnNames.Add(tablename + "." + a);
            foreach (var a in this.columnTypes)
                newtable.columnTypes.Add(a);
            foreach (var a in anotherTable.columnNames)
                newtable.columnNames.Add(tablename + "." + a);
            foreach (var a in anotherTable.columnTypes)
                newtable.columnTypes.Add(a);

            if (cond == null)
                cond = calcond(anotherTable.columnNames);
            if (cond.Count != 0)
            {
                List<List<Element>> correspondA = getCorrespon(cond, 0);
                List<List<Element>> correspondB = getCorrespon(cond, 1, anotherTable);
                QuickSort(correspondA, 0, correspondA.Count - 1, this);
                QuickSort(correspondB, 0, correspondB.Count - 1, anotherTable);

                int threadCount = Environment.ProcessorCount;
                Thread[] threadNum = new Thread[threadCount];
                List<dint> range = distinct(correspondA);
                for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
                {
                    JoinJudgeThreadObject p = new JoinJudgeThreadObject(threadCount, threadIndex, anotherTable, newtable, correspondA, correspondB, range);
                    threadNum[threadIndex] = new Thread(JoinJudgeThreadProc);
                    threadNum[threadIndex].Start(p);
                }
                for (int inde = 0; inde < threadCount; inde++)
                    threadNum[inde].Join();
            }
            else
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
        /// 无into的select语句
        /// </summary>
        /// <param name="fields">三元组数组，第一个表示字段，第二个表示别名</param>
        /// <param name="con">条件表达式</param>
        /// <returns></returns>
        public Table select(Tuple<string, string>[] fields, string con)
        {
            return null;
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
        }
        public List<List<Object>> getRow(int index)
        {
            return null;
        }
    }
}
