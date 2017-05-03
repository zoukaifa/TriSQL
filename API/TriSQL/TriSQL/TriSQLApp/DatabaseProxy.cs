using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;
using System.Threading;
namespace TriSQLApp
{
    class DatabaseProxy : DatabaseProxyBase
    {
        Semaphore sem = new Semaphore(0, 1);  //信号量
        Dictionary<int, List<List<long>>> idDict = new Dictionary<int, List<List<long>>>();  //收集cellid消息
        #region 邹开发 select
        public override void SelectFromClientHandler(SelectMessageReader request, SelectResponseWriter response)
        {
            idDict.Clear();
            for (int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                SelectMessageWriter smw = new SelectMessageWriter(request.columnNameList,
                    request.columnTypeList, request.cellIds, request.usedIndex, request.condition);
                Global.CloudStorage.SelectFromProxyToDatabaseServer(i, smw);
            }
            sem.WaitOne();  //等待服务器全部返回信息
            response.cellIds = new List<List<long>>();
            //将信息合并
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.cellIds.AddRange(idDict[i]);
            }
        }

        public override void SelectFromServerHandler(SelectResultResponseReader request)
        {
            idDict.Add(request.serverId, request.cellIds);  //将收到的信息加入字典
            if (idDict.Count == Global.ServerCount)
            {
                //所有服务器均完成查询，并已经发送给了proxy
                sem.Release();
            }
        }
        #endregion
        #region 田超 delete
        public override void DeleteFromClientHandler(DeleteMessageReader request, DeleteResponceWriter response)
        {
            idDict.Clear();
            for (int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                DeleteMessageWriter dmw = new DeleteMessageWriter(request.cellIds, request.columnTypeList, request.con);
                Global.CloudStorage.DeleteFromProxyToDatabaseServer(i, dmw);
            }
            sem.WaitOne();  //等待服务器全部返回信息
            response.cellIds = new List<List<long>>();
            //将信息合并
            for (int i = 0; i < Global.ServerCount; i++)
            {
                response.cellIds.AddRange(idDict[i]);
            }
        }
        public override void DeleteFromServerHandler(DeleteResultResponceReader request)
        {
            idDict.Add(request.serverId, request.cellIds);
            if (idDict.Count == Global.ServerCount)
            {
                //所有服务器均完成查询，并已经发送给了proxy
                sem.Release();
            }
        }
        public override void TruncateFromClientHandler(TruncateMessageReader request)
        {
            idDict.Clear();
            for (int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                TruncateMessageWriter tmw = new TruncateMessageWriter(request.cellIds);
                Global.CloudStorage.TruncateFromProxyToDatabaseServer(i, tmw);
            }
            sem.WaitOne();  //等待服务器全部返回信息

        }
        public override void TruncateFromServerHandler(TruncateResponceReader request)
        {
            idDict.Add(request.serverId, null);
            if (idDict.Count == Global.ServerCount)
            {
                //所有服务器均完成查询，并已经发送给了proxy
                sem.Release();
            }
        }
        #endregion

        #region join
        struct ClassifyObject
        {
            public int threadCount;
            public int threadIndex;
            public List<List<List<long>>> classify;
            public List<List<long>> cellids;
            public List<int> pos;
            public ClassifyObject(int threadCount, int threadIndex, List<List<long>> cellids, List<List<List<long>>> classify, List<int> pos)
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
        /// 分类器 把行按照服务器分类 
        /// </summary>
        /// <param name="CELLIDS"></param>
        /// <param name="cond">条件表达式表示用到的行</param>
        /// <returns></returns>
        private List<List<List<long>>> Classify(List<List<long>> CELLIDS, List<int> cond)
        {
            List<List<List<long>>> classify = new List<List<List<long>>>(Global.CloudStorage.ServerCount);
            for (int i = 0; i < Global.CloudStorage.ServerCount; i++)
            {
                classify.Add(new List<List<long>>());
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
            idDict.Add(request.serverid, request.celllids);
            if (idDict.Count == Global.ServerCount)
            {
                sem.Release();
            }
        }

        public override void JoinFromClientHandler(JoinMessageReader request, JoinResponceWriter response)
        {
            idDict.Clear();
            List<List<List<long>>> classify = Classify(request.cellidsB, request.condb);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                JoinMessageWriter msg = new JoinMessageWriter(request.cellidsA, classify[i], request.conda, request.condb);
                Global.CloudStorage.DoJoinFromProxyToDatabaseServer(i, msg);
            }
            sem.WaitOne();
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
            idDict.Clear();
            values.Clear();
            List<List<List<long>>> classify = Classify(request.celllids, request.cond);
            for (int i = 0; i < Global.ServerCount; i++)
            {
                TopKMessageWriter msg = new TopKMessageWriter(request.k, request.cond, classify[i]);
                Global.CloudStorage.TopKFromProxyToDatabaseServer(i, msg);
            }
            sem.WaitOne();
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

        Dictionary<int, List<int>> values = new Dictionary<int, List<int>>();
        public override void TopKFromServerHandler(TopKServerResponceReader request)
        {
            idDict.Add(request.serverid, request.celllids);
            values.Add(request.serverid, request.values);
            if (idDict.Count == Global.ServerCount)
            {
                sem.Release();
            }
        }
        #endregion


    }
}
