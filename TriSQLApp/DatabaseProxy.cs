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
    }
}