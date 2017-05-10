using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using System.Threading;
using System.Collections.Concurrent;
namespace TriSQLApp
{
    class DatabaseProxy : DatabaseProxyBase
    {
        Semaphore sem;  //信号量
        ConcurrentDictionary<int, List<List<long>>> idDict = new ConcurrentDictionary<int, List<List<long>>>();  //收集cellid消息
        public DatabaseProxy(): base()
        {
            sem = new Semaphore(0, Global.ServerCount);
        }
        #region 用于select和delete的classify
        struct ClassifyMessage
        {
            public int threadCount;
            public int threadIndex;
            public List<ConcurrentBag<List<long>>> classify;
            public List<List<long>> cellids;
            public ClassifyMessage(int threadCount, int threadIndex, List<List<long>> cellids, List<ConcurrentBag<List<long>>> classify)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.cellids = cellids;
                this.classify = classify;
            }
        }
        void ClassifyThread(object classifyMessage)
        {
            ClassifyMessage message = (ClassifyMessage)classifyMessage;
            int start = -1;
            int end = -1;
            int c = message.cellids.Count;
            int ele = c / message.threadCount;
            if (message.threadCount != message.threadIndex + 1)
            {
                start = message.threadIndex * ele;
                end = start + ele - 1;
            }
            else
            {
                start = message.threadIndex * ele;
                end = c - 1;
            }
            for (int i = start; i <= end; i++)
            {
                int serverid = Global.CloudStorage.GetServerIdByCellId(message.cellids[i][0]);
                message.classify[serverid].Add(message.cellids[i]);
            }
        }
        private List<ConcurrentBag<List<long>>> Classify(List<List<long>> CELLIDS)
        {
            List<ConcurrentBag<List<long>>> classify = new List<ConcurrentBag<List<long>>>(Global.CloudStorage.ServerCount);
            for (int i = 0; i < Global.CloudStorage.ServerCount; i++)
            {
                classify.Add(new ConcurrentBag<List<long>>());
            }
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                ClassifyMessage p = new ClassifyMessage(threadCount, threadIndex, CELLIDS, classify);
                threadNum[threadIndex] = new Thread(ClassifyThread);
                threadNum[threadIndex].Start(p);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            return classify;
        }
        #endregion


        #region 邹开发 select
        public override void SelectFromClientHandler(SelectMessageReader request, SelectResponseWriter response)
        {
            Console.WriteLine(DateTime.Now.ToString() + "收到客户端的Select请求，清空Dict");
            idDict.Clear();
            List<ConcurrentBag<List<long>>> classify = Classify(request.cellIds);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                SelectMessageWriter smw = new SelectMessageWriter(request.columnNameList,
                    request.columnTypeList, classify[i].ToList(), request.usedIndex, request.condition, request.nestedColumns);
                Global.CloudStorage.SelectFromProxyToDatabaseServer(i, smw);
            }
            for(int i = 0; i < Global.ServerCount; i++)
            {
                sem.WaitOne();
            }
            response.cellIds = new List<List<long>>();
            //将信息合并
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.cellIds.AddRange(idDict[i]);
            }
        }

        public override void SelectFromServerHandler(SelectResultResponseReader request)
        {
            idDict.GetOrAdd(request.serverId, request.cellIds);  //将收到的信息加入字典
            sem.Release();
        }
        #endregion
        #region 田超 delete
        public override void DeleteFromClientHandler(DeleteMessageReader request, DeleteResponceWriter response)
        {
            Console.WriteLine(DateTime.Now.ToString()+"收到客户端的Delete请求，清空Dict");
            idDict.Clear();
            List<ConcurrentBag<List<long>>> classify = Classify(request.cellIds);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                DeleteMessageWriter dmw = new DeleteMessageWriter(classify[i].ToList(), request.columnTypeList, request.con);
                Global.CloudStorage.DeleteFromProxyToDatabaseServer(i, dmw);
            }
            for (int i = 0; i < Global.ServerCount; i++)
            {
                sem.WaitOne();
            }
            response.cellIds = new List<List<long>>();
            //将信息合并
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.cellIds.AddRange(idDict[i]);
            }
        }
        public override void DeleteFromServerHandler(DeleteResultResponceReader request)
        {
            idDict.GetOrAdd(request.serverId, request.cellIds);
            sem.Release();
        }
        #endregion

        #region join
        struct ClassifyObject
        {
            public int threadCount;
            public int threadIndex;
            public List<ConcurrentBag<List<long>>> classify;
            public List<List<long>> cellids;
            public List<int> pos;
            public ClassifyObject(int threadCount, int threadIndex, List<List<long>> cellids, List<ConcurrentBag<List<long>>> classify, List<int> pos)
            {
                this.threadCount = threadCount;
                this.threadIndex = threadIndex;
                this.cellids = cellids;
                this.classify = classify;
                this.pos = pos;
            }
        }
        void ClassifyThreadProc(object par)
        {
            ClassifyObject p = (ClassifyObject)par;
            int start = -1;
            int end = -1;
            int c = p.cellids.Count;
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
            int temp = 0;
            if (p.pos != null && p.pos.Count != 0)
                temp = p.pos[0];
            for (int i = start; i <= end; i++)
            {
                int serverid = Global.CloudStorage.GetServerIdByCellId(p.cellids[i][temp]);
                p.classify[serverid].Add(p.cellids[i]);
            }
        }
        /// <summary>
        /// 分类器 把行按照服务器分类 多线程
        /// </summary>
        /// <param name="CELLIDS"></param>
        /// <param name="cond">条件表达式表示用到的行</param>
        /// <returns></returns>
        private List<ConcurrentBag<List<long>>> Classify(List<List<long>> CELLIDS, List<int> cond)
        {
            List<ConcurrentBag<List<long>>> classify = new List<ConcurrentBag<List<long>>>(Global.CloudStorage.ServerCount);
            for (int i = 0; i < Global.CloudStorage.ServerCount; i++)
            {
                classify.Add(new ConcurrentBag<List<long>>());
            }
            int threadCount = Environment.ProcessorCount;
            Thread[] threadNum = new Thread[threadCount];
            for (int threadIndex = 0; threadIndex < threadCount; threadIndex++)
            {
                ClassifyObject p = new ClassifyObject(threadCount, threadIndex, CELLIDS, classify, cond);
                threadNum[threadIndex] = new Thread(ClassifyThreadProc);
                threadNum[threadIndex].Start(p);
            }
            for (int inde = 0; inde < threadCount; inde++)
                threadNum[inde].Join();
            return classify;
        }
        public override void REdoJoinFromServerHandler(JoinResponceReader request)
        {
            idDict.GetOrAdd(request.serverid, request.celllids);
            sem.Release();
        }

        public override void JoinFromClientHandler(JoinMessageReader request, JoinResponceWriter response)
        {
            Console.WriteLine(DateTime.Now.ToString() + "收到客户端的Join请求，清空Dict");
            idDict.Clear();
            List<ConcurrentBag<List<long>>> classify = Classify(request.cellidsB, request.condb);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                JoinMessageWriter msg = new JoinMessageWriter(request.cellidsA, classify[i].ToList(), request.conda, request.condb);
                Global.CloudStorage.DoJoinFromProxyToDatabaseServer(i, msg);
            }
            for (int i = 0; i < Global.ServerCount; i++)
            {
                sem.WaitOne();
            }
            response.celllids = new List<List<long>>();
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.celllids.AddRange(idDict[i]);
            }
            response.serverid = 0;
        }
        #endregion
        #region topk
        public override void TopKFromClientHandler(TopKMessageReader request, TopKResponceWriter response)
        {
            Console.WriteLine(DateTime.Now.ToString() + "收到客户端的TopK请求，清空Dict");
            idDict.Clear();
            values.Clear();
            List<ConcurrentBag<List<long>>> classify = Classify(request.celllids, request.cond);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                TopKMessageWriter msg = new TopKMessageWriter(request.k, request.cond, classify[i].ToList());
                Global.CloudStorage.TopKFromProxyToDatabaseServer(i, msg);
            }
            for (int i = 0; i < Global.ServerCount; i++)
            {
                sem.WaitOne();
            }
            int k = request.k;
            int[] stage = new int[k];
            for (int i = 0; i < k; i++)
            {
                stage[i] = 0;
            }
            response.celllids = new List<List<long>>();
            for (int i = 0; i < k; i++)
            {
                int max = values[0][stage[0]];
                int maxserver = 0;
                for (int j = 1; j < Global.ServerCount; j++)
                {
                    if (max < values[j][stage[j]])
                    {
                        max = values[j][stage[j]];
                        maxserver = j;
                    }
                }
                response.celllids.Add(idDict[maxserver][stage[maxserver]]);
                stage[maxserver]++;
            }
        }

        ConcurrentDictionary<int, List<int>> values = new ConcurrentDictionary<int, List<int>>();
        public override void TopKFromServerHandler(TopKServerResponceReader request)
        {
            idDict.GetOrAdd(request.serverid, request.celllids);
            values.GetOrAdd(request.serverid, request.values);
            sem.Release();
        }
        #endregion
        #region union
        public override void UnionFromClientHandler(UnionMessageReader request, UnionResponseWriter response)
        {
            idDict.Clear();
            List<ConcurrentBag<List<long>>> classify = Classify(request.cellidsB, null);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                UnionMessageWriter msg = new UnionMessageWriter(request.cellidsA, classify[i].ToList());
                Global.CloudStorage.UnionFromProxyToDatabaseServer(i, msg);
            }
            for (int i = 0; i < Global.ServerCount; i++)
            {
                sem.WaitOne();
            }
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.cellids.AddRange(idDict[i]);
            }
        }

        public override void UnionFromServerHandler(UnionServerResponseReader request)
        {
            idDict.GetOrAdd(request.serverid, request.cellids);
            sem.Release();
        }
        #endregion
    }
}
