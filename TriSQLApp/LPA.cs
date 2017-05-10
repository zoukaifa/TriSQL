using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TriSQLApp
{
    class LPA
    {
        private Database db;
        private Table graph, label, temp;
        private const int nodeNum = 14;
        private const int iteNum = 30;

        public void create()
        {
            if(Database.exists("GraphEngine"))
            {
                Database.dropDatabase("GraphEngine");
            }
            db = Database.createDatabase("GraphEngine");
            graph = db.createTable("Graph", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "node1", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "node2", 0));
            label = db.createTable("Label", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "node", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "label", 0));
            temp = db.createTable("Temp", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "label", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "num", 0));
            initData();
        }
        /// <summary>
        /// 建表
        /// </summary>
        public void initTable()
        {
            //建立数据库
            db = new Database("GraphEngine");
            graph = new Table("Graph");
            label = new Table("Label");
            temp = new Table("Temp");
        }
        /// <summary>
        /// 插入数据
        /// </summary>
        public void initData()
        {
            graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 1 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 2 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 0, 3 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 2 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 3 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 4 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 5 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 4 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 11 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 6 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 7 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 7 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 8 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 11 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 11 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 12 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 10, 13 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 12 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 13 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 13 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 1, 0 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 2, 0 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 0 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 2, 1 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 3, 1 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 4, 1 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 1 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 4, 3 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 3 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 3 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 5 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 5 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 5 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 7, 6 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 6 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 7 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 8 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 11, 10 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 10 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 10 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 12, 11 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 11 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 13, 12 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 5, 8 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 8, 5 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 6, 9 });
            graph.insert(new string[] { "node1", "node2" }, new object[] { 9, 6 });
            for(int i = nodeNum;i > 0; i--)
            {
                label.insert(new string[] { "node", "label" }, new object[] { nodeNum - i, nodeNum - i });
                temp.insert(new string[] { "label", "num"}, new object[] { nodeNum - i, 0});
            }
        }

        public void cal()
        {
            Random random = new Random();
            object[][] values = new object[nodeNum][];
            for (int j = nodeNum; j > 0; j--)
            {
                values[j - 1] = new object[] { nodeNum - j, 0};
            }
            List<dint> con = new List<dint> { new dint(1, 0) };
            for (int i = 0; i < iteNum; i++)
            {
                for(int k = nodeNum; k > 0; k--)
                {
                    temp.delete(null);       
                    temp.insert(new string[] { "label", "num"}, values);
                    //select label.label from graph inner join label on graph.node2 = label.node where graph.node1 = 14 - @nodeNum;
                    Table labelNum = graph.innerJoinOnCluster(label, "LabelNum", con);
                    labelNum = labelNum.select(new Tuple<string, string>[]
                    { new Tuple<string, string>("Label.label", "")
                    }, " Graph.node1 == " + (nodeNum - k)+" ");
                    List<Object> labels = labelNum.getColumn("Label.label");
                    //计算数量
                    foreach (Object la in labels)
                    {
                        temp.update("num", 1, '+', 1, "label == " + la);
                    }
                    Table topLabel = new Table(temp.topK(nodeNum, new string[] { "num" }), temp.getColumnTypes(),
                        temp.getColumnNames());
                    int count = 0;
                    labels = topLabel.getColumn("num");  //计算有多少个重复的
                    while (count < labels.Count && labels[count].Equals(labels[0]))
                    {
                        count++;
                    }
                    //随机选择0 - count-1
                    int newLabel = (int)(topLabel.getColumn("label")[random.Next() % count]);
                    //update label set label = nod where node = 14-@nodeNum;
                    label.update("label", 0, '=', newLabel, "node == " + (14 - k));
                }
                Console.WriteLine(i);
                label.printTable();
            }
        }

        public void delete()
        {
            Database.dropDatabase("GraphEngine");
        }

        public void start()
        {
            initTable();
            initData();
            cal();
            label.printTable();
            delete();
        }
    }
}
