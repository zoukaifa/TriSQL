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
        Dictionary<int, List<List<long>>> messageDic = new Dictionary<int, List<List<long>>>();  //收集消息
        
        public override void SelectFromClientHandler(SelectMessageReader request, SelectResponseWriter response)
        {
            messageDic.Clear();
            for(int i = 0; i < Global.ServerCount; i++)
            {
                //每个服务器挨个发送
                SelectMessageWriter smw = new SelectMessageWriter(request.rowIds,
                    request.tableNames, request.tableIds, request.indexes, request.columnTypes,
                    request.columnNames, request.primaryIndexes, request.defaultValues, request.condition);
                Global.CloudStorage.SelectFromProxyToDatabaseServer(i, smw);
            }
            sem.WaitOne();  //等待服务器全部返回信息
            List<List<long>> rowIds = new List<List<long>>();
            for(int i = 0; i < request.rowIds.Count; i++)
            {
                rowIds.Add(new List<long>());
            }
            //将信息合并
            for(int i = 0; i < Global.ServerCount; i++)
            {
                List<List<long>> partRowIds = messageDic[i];
                for (int j = 0; j < request.rowIds.Count; j++)
                {
                    rowIds[j].AddRange(partRowIds[j]);
                }
            }
            response.rowIds = rowIds;
        }

        public override void SelectFromServerHandler(SelectResultResponseReader request)
        {
            messageDic.Add(request.serverId, request.rowIds);  //将收到的信息加入字典
            if (messageDic.Count == Global.ServerCount)
            {
                //所有服务器均完成查询，并已经发送给了proxy
                sem.Release();
            }
        }
    }
}
