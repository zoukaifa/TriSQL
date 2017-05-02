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
        Dictionary<int, List<List<long>>> idDict = new Dictionary<int, List<List<long>>>();  //收集消息
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
            if (p.pos != null)
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
        public override void SelectFromClientHandler(SelectMessageReader request, SelectResponseWriter response)
        {

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
        #region join
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
            List<List<List<long>>> classify = Classify(request.celllids, request.cond);
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

        public override void TopKFromServerHandler(TopKResponceReader request)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}