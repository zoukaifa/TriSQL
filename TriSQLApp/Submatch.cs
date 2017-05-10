using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Trinity;
using Trinity.Core.Lib;
using TriSQL;

namespace TriSQLApp
{
    class str2
    {
        public string s1;
        public string s2;

        public str2(string s1, string s2)
        {
            this.s1 = s1;
            this.s2 = s2;
        }
    };
    class SubgraphMatch
    {
        private Database db;
        private Table graph, label;
        public void usedatabase()
        {
            db = new Database("submatch");
            graph = new Table("Graph");
            label = new Table("Label");
            graph.printTable();
            label.printTable();
        }
        public void init()
        {
            db = Database.createDatabase("submatch");
            graph = db.createTable("Graph", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "node1", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "node2", 0));
            label = db.createTable("Label", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "node", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "label", 0));
        }
        public void create()
        {
            try
            {
                StreamReader infile = new StreamReader("C:\\Users\\lining\\Desktop\\graph.txt");
                int n;
                string line = infile.ReadLine();
                n = int.Parse(line);
                Dictionary<string, int> M = new Dictionary<string, int>();
                List<str2> Q = new List<str2>();
                int g = 0;
                for (int i = 0; i < n; i++)
                {
                    string s1, s2;
                    line = infile.ReadLine();
                    s1 = line.Split(' ')[0];
                    s2 = line.Split(' ')[1];
                    str2 s = new str2(s1, s2);
                    Q.Add(s);
                    if (!M.ContainsKey(s1))
                    {
                        M[s1] = g++;
                    }
                    if (!M.ContainsKey(s2))
                    {
                        M[s1] = g++;
                    }
                }
                infile.Close();
                foreach (var a in M)
                {
                    label.insert(new string[] { "node", "label" }, new object[] { a.Value + 1, (int)a.Key[0] });
                }
                foreach (var a in Q)
                {
                    graph.insert(new string[] { "node1", "node2" }, new object[] { M[a.s1] + 1, M[a.s2] + 1 });
                    graph.insert(new string[] { "node1", "node2" }, new object[] { M[a.s2] + 1, M[a.s1] + 1 });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        public void query()
        {
            int[] q1 = new int[4];
            q1[0] = (int)'b';
            q1[1] = (int)'a';
            q1[2] = (int)'e';
            q1[3] = (int)'c';
            int[] q2 = new int[3];
            q2[0] = (int)'e';
            q2[1] = (int)'a';
            q2[2] = (int)'d';
            int[] q3 = new int[2];
            q3[0] = (int)'c';
            q3[1] = (int)'d';
            
            int[] num = new int[3] { 4, 3, 2 };

            List<int[]> Q = new List<int[]>();
            Q.Add(q1);
            Q.Add(q2);
            Q.Add(q3);

            if (db.tableExists("ttt")) db.dropTable("ttt");
            Table ttt = db.createTable("ttt", new string[] { },
                new Tuple<int, string, object>(FieldType.INTEGER, "node1", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "node2", 0),
                new Tuple<int, string, object>(FieldType.INTEGER, "label", 0));

            const int querynum = 3;
            for (int i = 0; i < querynum; i++)
            {
                ttt.delete(null);
                string cond = "label == " + Q[i][0];
                Table ta = label.select(new Tuple<string, string>[] {
                    new Tuple<string, string>("node", null) }, cond);
                ta.setTableName("ta");

                List<dint> con = new List<dint>();
                con.Add(new dint(0, 0));
                Table tb = ta.innerJoinOnCluster(graph, "tb", con);
                tb = tb.select(new Tuple<string, string>[] {
                    new Tuple<string,string> ("Graph.node1",null),
                    new Tuple<string,string> ("Graph.node2",null) }, null
                );

                con.Clear();
                con.Add(new dint(1, 0));
                Table tc = tb.innerJoinOnCluster(label, "tc", con);
                List<int> condList = new List<int>();
                for (int j = 1; j < num[i]; j++)
                    condList.Add(Q[i][j]);
                tc = tc.select(new Tuple<string, string>[] {
                    new Tuple<string,string> ("tb.Graph.node1",null),
                    new Tuple<string,string> ("tb.Graph.node2",null),
                    new Tuple<string,string> ("Label.label",null)}, "Label.label in tmp",
                    condList
                );

                ttt.insert(tc.getColumnNamesString(), tc);
                if (db.tableExists(i.ToString())) db.dropTable(i.ToString());

                Tuple<int, string, object>[] fields = new Tuple<int, string, object>[Q[i].Length];
                for (int j = 0; j < Q[i].Length; j++)
                {
                    fields[j] = new Tuple<int, string, object>(FieldType.INTEGER, Q[i][j].ToString(), 0);
                }
                Table q = db.createTable(i.ToString(), new string[] { }, fields);

                ///ttt分开到->q
                Table jjj = ttt.select(new Tuple<string, string>[] {
                    new Tuple<string, string>("node1", null),
                    new Tuple<string, string>("node2", null) }, "label == " + Q[i][1]);
                //jjj.setTableName("0");
                for (int j = 2; j < Q[i].Length; j++)
                {
                    Table temp = ttt.select(new Tuple<string, string>[] {
                    new Tuple<string, string>("node1", null),
                    new Tuple<string, string>("node2", null) }, "label == " + Q[i][j]);
                    temp.setTableName(j.ToString());

                    con.Clear();
                    con.Add(new dint(0,0));
                    int jjjsize = jjj.getColumnNames().Count;
                    List<int> conlist = new List<int>();
                    for (int k = 0; k < jjjsize; k++)
                    {
                        conlist.Add(k);
                    }
                    conlist.Add(jjjsize + 1);
                    jjj = jjj.innerJoinOnCluster(temp, j.ToString(), con).select(conlist);
                }
                q.insert(jjj.getColumnNamesString(), jjj);
            }

            ///最后的join
            Table NEW = new Table("0");
            for (int z = 1; z < querynum; z++)
            {//不断更新num[0],Q[0],q1
                int equalsize = 0;
                int stepa = num[0];
                int stepb = num[z];
                int[] qa;
                int[] qb;
                qa = Q[0];
                qb = Q[z];
                List<dint> equal = new List<dint>();
                /*pre process*/
                for (int i = 0; i < stepa; i++)
                {
                    for (int j = 0; j < stepb; j++)
                    {
                        if (qa[i]==qb[j])
                        {
                            equal.Add(new dint(i,j));
                            equalsize++;
                        }
                    }
                }
                equal.Sort();
                /*init result*/
                int stepres;
                stepres = stepa + stepb - equalsize;
                int[] resQ = new int[stepres];
                int temp = 0, temp1 = 0;
                for (int k = 0; k < stepa + stepb; k++)
                {
                    if (k < stepa)
                    {
                        resQ[k] = qa[k];
                        temp = k;
                    }
                    else if (temp1 < equalsize && equal[temp1].b != k - stepa)
                    {
                        resQ[(++temp)] = qb[(k - stepa)];
                    }
                    else
                    {
                        temp1++;
                    }
                }

                Table NEW2 = new Table(z.ToString());
                List<dint> con = new List<dint>();
                for(int i = 0; i< equalsize; i++)
                {
                    con.Add(equal[i]);
                }
                NEW.printTable();
                Console.WriteLine("-----------");
                NEW2.printTable();
                NEW = NEW.innerJoinOnCluster(NEW2, "", con);
                List<int> conlist = new List<int>();
                for (int k = 0; k < NEW.getColumnNames().Count; k++)
                {
                    bool flag = true;
                    for (int p = 0; p <equalsize; p++)
                    {
                        if (equal[p].b+stepa == k)
                        {
                            flag = false;
                        }
                    }
                    if (flag)
                        conlist.Add(k);
                }
                NEW = NEW.select(conlist);
            }
            NEW.printTable();
            Console.ReadKey();
        }
    }
}
