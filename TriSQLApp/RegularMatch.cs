using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;
using System.IO;


namespace TriSQLApp
{
    class RegularMatch
    {
        public int NUMBER = 100;
        public int num = 16;
        public string[] sname = new string[] { "N1", "N2" };
        public Table tablea;
        public Table tableb;
        public Table tablec;
        public Table tabled;
        public Table tablee;

        public Table Fa;
        public Table Fb;
        public Table Fc;
        public Table Fd;
        public Table Fe;

        public List<List<long>> result = new List<List<long>>();
        public struct node
        {
            public int node1;
            public int node2;
            public char value;
        }
        public struct MN
        {
            public char c;
            public int flag;
        }

        public RegularMatch(string s)
        {
            Database db = Database.createDatabase("TIANCHAO");
            this.tablea = db.createTable("tablea", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            this.tableb = db.createTable("tableb", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            this.tablec = db.createTable("tablec", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            this.tabled = db.createTable("tabled", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            this.tablee = db.createTable("tablee", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            initial();
            match(s);
        }
        public void initial()
        {

            initialTablea(this.tablea);
            initialTableb(this.tableb);
            initialTablec(this.tablec);
            initialTabled(this.tabled);
            initialTablee(this.tablee);
        }
        public void initialTablea(Table table)
        {
            table.insert(sname, new object[] { 1, 2 });
            table.insert(sname, new object[] { 2, 6 });
            table.insert(sname, new object[] { 3, 4 });
            table.insert(sname, new object[] { 6, 8 });
            table.insert(sname, new object[] { 7, 8 });
            table.insert(sname, new object[] { 8, 9 });
        }
        public void initialTableb(Table table)
        {
            table.insert(sname, new object[] { 1, 4 });
            table.insert(sname, new object[] { 2, 3 });
            table.insert(sname, new object[] { 4, 7 });
            table.insert(sname, new object[] { 8, 10 });
        }
        public void initialTablec(Table table)
        {
            table.insert(sname, new object[] { 5, 9 });
        }
        public void initialTabled(Table table)
        {
            table.insert(sname, new object[] { 3, 6 });
            table.insert(sname, new object[] { 6, 7 });
            table.insert(sname, new object[] { 10, 11 });
        }
        public void initialTablee(Table table)
        {
            table.insert(sname, new object[] { 2, 5 });
            table.insert(sname, new object[] { 5, 8 });
        }
        public void ManagePlus(char c)
        {
            // outplus << "CREATE TABLE T" << c << "(N1 int,N2 int);" << endl;
            // outplus << "INSERT INTO T" << c << " (SELECT * FROM table_" << c << ");" << endl;
            // outplus << "INSERT INTO T" << c << endl;
            // outplus << "(SELECT table_" << c << ".N1,table_" << c << "2.N2 FROM table_" << c << " INNER JOIN table_" << c << " as table_" << c << "2 ON table_" << c << ".N2 = table_" << c << "2.N1);" << endl;
            if (c == 'a')
            {
                Database.getCurrentDatabase().createTable("Ta", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Table Ta = new Table("Ta");
                //将原有的tablea 插入到ta
                Table tempTable1 = tablea.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                Ta.insert(sname, tempTable1);
                Table tablea2 = new Table(tablea, "tablea2");
                dint n = new dint();
                n.a = 1;
                n.b = 0;
                List<dint> d = new List<dint>();
                d.Add(n);
                Table tablejoin = tablea.innerJoin(tablea2, "tablejoin", d);
                Table tempTable2 = tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("tablea.N1", null),
                new Tuple<string, string>("tablea2.N2", null)}, null);
                Ta.insert(sname, tempTable2);

                for (int i = 1; i < num / 2; i++)
                {

                    Table tablejoin1 = Ta.innerJoin(tablea, "tablejoin", d);
                    Table tempTable3 = tablejoin1.select(new Tuple<string, string>[] { new Tuple<string, string>("Ta.N1", null), new Tuple<string, string>("tablea.N2", null) }, null);
                    Ta.insert(sname, tempTable3);
                }
                Database.getCurrentDatabase().createTable("Fa", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Fa = new Table("Fa");
                Fa.insert(sname, Ta.select(new Tuple<string, string>[] { new Tuple<string, string>("Ta.N1", null), new Tuple<string, string>("Ta.N2", null) }, null));
                Database.getCurrentDatabase().dropTable("Ta");
            }
            else if (c == 'b')
            {
                Database.getCurrentDatabase().createTable("Tb", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Table Tb = new Table("Tb");
                //将原有的tablea 插入到ta

                Table tempTable1 = tableb.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                Tb.insert(sname, tempTable1);
                Table tableb2 = new Table(tableb, "tableb2");
                dint n = new dint();
                n.a = 1;
                n.b = 0;
                List<dint> d = new List<dint>();
                d.Add(n);
                Table tablejoin = tableb.innerJoin(tableb2, "tablejoin", d);

                Table tempTable2 = tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("tableb.N1", null), new Tuple<string, string>("tableb2.N2", null) }, null);
                Tb.insert(sname, tempTable2);

                for (int i = 1; i < num / 2; i++)
                {

                    Table tablejoin1 = Tb.innerJoin(tableb, "tablejoin", d);
                    Table tempTable3 = tablejoin1.select(new Tuple<string, string>[] { new Tuple<string, string>("Tb.N1", null), new Tuple<string, string>("tableb.N2", null) }, null);
                    Tb.insert(sname, tempTable3);
                }
                Database.getCurrentDatabase().createTable("Fb", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Fb = new Table("Fb");
                Fb.insert(sname, Tb.select(new Tuple<string, string>[] { new Tuple<string, string>("Tb.N1", null), new Tuple<string, string>("Tb.N2", null) }, null));
                Database.getCurrentDatabase().dropTable("Tb");
            }
            else if (c == 'c')
            {
                Database.getCurrentDatabase().createTable("Tc", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Table Tc = new Table("Tc");
                //将原有的tablea 插入到ta

                Table tempTable1 = tablec.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                Tc.insert(sname, tempTable1);
                Table tablec2 = new Table(tablec, "tablec2");
                dint n = new dint();
                n.a = 1;
                n.b = 0;
                List<dint> d = new List<dint>();
                d.Add(n);
                Table tablejoin = tablec.innerJoin(tablec2, "tablejoin", d);

                Table tempTable2 = tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("tablec.N1", null), new Tuple<string, string>("tablec2.N2", null) }, null);
                Tc.insert(sname, tempTable2);

                for (int i = 1; i < num / 2; i++)
                {

                    Table tablejoin1 = Tc.innerJoin(tablec, "tablejoin", d);
                    Table tempTable3 = tablejoin1.select(new Tuple<string, string>[] { new Tuple<string, string>("Tc.N1", null), new Tuple<string, string>("tablec.N2", null) }, null);
                    Tc.insert(sname, tempTable3);
                }
                Database.getCurrentDatabase().createTable("Fc", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Fc = new Table("Fc");
                Fc.insert(sname, Tc.select(new Tuple<string, string>[] { new Tuple<string, string>("Tc.N1", null), new Tuple<string, string>("Tc.N2", null) }, null));
                Database.getCurrentDatabase().dropTable("Tc");
            }
            else if (c == 'd')
            {
                Database.getCurrentDatabase().createTable("Td", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Table Td = new Table("Td");
                //将原有的tablea 插入到ta

                Table tempTable1 = tabled.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                Td.insert(sname, tempTable1);
                Table tabled2 = new Table(tabled, "tabled2");
                dint n = new dint();
                n.a = 1;
                n.b = 0;
                List<dint> d = new List<dint>();
                d.Add(n);
                Table tablejoin = tablea.innerJoin(tabled2, "tablejoin", d);

                Table tempTable2 = tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("tabled.N1", null), new Tuple<string, string>("tabled2.N2", null) }, null);
                Td.insert(sname, tempTable2);

                for (int i = 1; i < num / 2; i++)
                {

                    Table tablejoin1 = Td.innerJoin(tabled, "tablejoin", d);
                    Table tempTable3 = tablejoin1.select(new Tuple<string, string>[] { new Tuple<string, string>("Td.N1", null), new Tuple<string, string>("tabled.N2", null) }, null);
                    Td.insert(sname, tempTable3);
                }
                Database.getCurrentDatabase().createTable("Fd", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Fd = new Table("Fd");
                Fd.insert(sname, Td.select(new Tuple<string, string>[] { new Tuple<string, string>("Td.N1", null), new Tuple<string, string>("Td.N2", null) }, null));
                Database.getCurrentDatabase().dropTable("Td");
            }
            else
            {
                Database.getCurrentDatabase().createTable("Te", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Table Te = new Table("Te");
                //将原有的tablea 插入到ta

                Table tempTable1 = tablee.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                Te.insert(sname, tempTable1);
                Table tablee2 = new Table(tablee, "tablee2");
                dint n = new dint();
                n.a = 1;
                n.b = 0;
                List<dint> d = new List<dint>();
                d.Add(n);
                Table tablejoin = tablea.innerJoin(tablee2, "tablejoin", d);

                Table tempTable2 = tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("tablee.N1", null), new Tuple<string, string>("tablee2.N2", null) }, null);
                Te.insert(sname, tempTable2);

                for (int i = 1; i < num / 2; i++)
                {

                    Table tablejoin1 = Te.innerJoin(tablee, "tablejoin", d);
                    Table tempTable3 = tablejoin1.select(new Tuple<string, string>[] { new Tuple<string, string>("Te.N1", null), new Tuple<string, string>("tablee.N2", null) }, null);
                    Te.insert(sname, tempTable3);
                }
                Database.getCurrentDatabase().createTable("Fe", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                Fe = new Table("Fe");
                Fe.insert(sname, Te.select(new Tuple<string, string>[] { new Tuple<string, string>("Te.N1", null), new Tuple<string, string>("Te.N2", null) }, null));
                Database.getCurrentDatabase().dropTable("Te");
            }

            //  for (int i = 1; i < num / 2; i++)
            //  {
            // outplus << "INSERT INTO T" << c << endl;
            // outplus << "(SELECT T" << c << ".N1" << ",table_" << c << ".N2 FROM T" << c << " INNER JOIN table_" << c << " ON T" << c << ".N2 = table_" << c << ".N1);" << endl;
            //  }
            // outplus << "CREATE TABLE F" << c << "(N1 int,N2 int);" << endl;
            // outplus << "INSERT INTO F" << c << endl;
            // outplus << "SELECT DISTINCT T" << c << ".N1,T" << c << ".N2 FROM T" << c << ";" << endl;
            //outplus << "DROP TABLE T" << c << ";" << endl;

        }
        public void match(string s)
        {
            MN[] all = new MN[NUMBER];
            MN[] note = new MN[NUMBER];
            char c;
            for (int i = 0; i < s.Length; i++)
            {
                note[i].flag = 0;
                note[i].c = ' ';
                all[i].c = s[i];
                all[i].flag = 1;
            }
            int m = 0;//最后一共可以拆分为m+1个串
            for (int i = 0; i < s.Length; i++)
            {
                if (all[i].c != '*' && all[i].c != '+')
                {
                    note[m].flag++;
                }
                else if (all[i].c == '*')
                {
                    note[m].flag--;
                    //表示a*出现在字符串首
                    if (note[m].flag == 0)
                    {
                        note[m].c = '*';
                        m++;
                    }
                    else
                    {
                        m++;
                        note[m].c = '*';
                        m++;
                    }
                }
                else
                {
                    note[m].flag--;
                    if (note[m].flag == 0)
                    {
                        note[m].c = '+';
                        m++;
                    }
                    else
                    {
                        m++;
                        note[m].c = '+';
                        m++;
                    }
                }
            }
            //用来处理字符串中出现的所有的 c* 和 c+ 生成表Fc
            for (int i = 0; i < s.Length; i++)
            {
                if ((all[i].c == '*' || all[i].c == '+') && all[i - 1].flag == 1)
                {
                    //对all[i-1]进行* +处理
                    c = all[i - 1].c;
                    ManagePlus(c);
                    for (int j = 0; j < s.Length; j++)
                    {
                        if (all[j].c == c && all[j + 1].c == '*')
                        {
                            all[j].flag = 0;
                        }
                    }
                }
            }
            //利用表的join操作匹配非*和+
            //outplus << "CREATE TABLE FINAL(N1 int,N2 int);" << endl;
            //outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
            //outplus << "CREATE TABLE TP(N1 int,N2 int);" << endl;
            Database.getCurrentDatabase().createTable("FINAL", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            Database.getCurrentDatabase().createTable("TEMP", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            Database.getCurrentDatabase().createTable("TP", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
            Table FINAL = new Table("FINAL");
            Table TEMP = new Table("TEMP");
            Table TP = new Table("TP");
            int count = 0;
            if (note[0].flag != 0)
            {
                // outplus << "INSERT INTO TEMP (SELECT * FROM table_" << s[0] << ");" << endl;
                if (s[0] == 'a')
                {
                    Table tempTable1 = tablea.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'b')
                {
                    Table tempTable1 = tableb.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'c')
                {
                    Table tempTable1 = tablec.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'd')
                {
                    Table tempTable1 = tabled.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'e')
                {
                    Table tempTable1 = tablee.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }

            }
            else if (note[0].flag == 0 && note[0].c == '*')
            {
                // outplus << "INSERT INTO TEMP (SELECT * FROM F" << s[0] << ");" << endl;
                if (s[0] == 'a')
                {
                    Table tempTable1 = Fa.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'b')
                {
                    Table tempTable1 = Fb.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'c')
                {
                    Table tempTable1 = Fc.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'd')
                {
                    Table tempTable1 = Fd.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'e')
                {
                    Table tempTable1 = Fe.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }

            }
            else if (note[0].flag == 0 && note[0].c == '+')
            {
                // outplus << "INSERT INTO TEMP (SELECT * FROM F" << s[0] << ");" << endl;
                if (s[0] == 'a')
                {
                    Table tempTable1 = Fa.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'b')
                {
                    Table tempTable1 = Fb.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'c')
                {
                    Table tempTable1 = Fc.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'd')
                {
                    Table tempTable1 = Fd.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
                else if (s[0] == 'e')
                {
                    Table tempTable1 = Fe.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null);
                    TEMP.insert(sname, tempTable1);
                }
            }

            for (int i = 0; i < m + 1; i++)
            {
                if (note[i].flag == 0 && note[i].c == '*')
                {
                    if (i == 0)
                    {
                        count = count + 2;
                    }
                    else
                    {
                        // outplus << "INSERT INTO TP (SELECT * FROM TEMP);" << endl;
                        // outplus << "INSERT INTO TEMP(SELECT TEMP.N1,F" << s[count] << ".N2 FROM TEMP INNER JOIN F" << s[count] << " ON TEMP.N2=F" << s[count] << ".N1);" << endl;
                        TP.insert(sname, TEMP.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                        List<dint> din = new List<dint>();
                        dint n = new dint();
                        n.a = 1;
                        n.b = 0;
                        din.Add(n);
                        if (s[count] == 'a')
                        {
                            Table tablejoin = TEMP.innerJoin(Fa, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fa.N2", null) }, null));
                        }
                        else if (s[count] == 'b')
                        {
                            Table tablejoin = TEMP.innerJoin(Fb, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fb.N2", null) }, null));
                        }
                        else if (s[count] == 'c')
                        {
                            Table tablejoin = TEMP.innerJoin(Fc, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fc.N2", null) }, null));
                        }
                        else if (s[count] == 'd')
                        {
                            Table tablejoin = TEMP.innerJoin(Fd, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fd.N2", null) }, null));
                        }
                        else if (s[count] == 'e')
                        {
                            Table tablejoin = TEMP.innerJoin(Fe, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fe.N2", null) }, null));
                        }
                        count = count + 2;
                        // outplus << "INSERT INTO TEMP(SELECT * FROM TP UNION SELECT * FROM TEMP);" << endl;
                        TEMP.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null).union_distinct(TP.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                        //union tp和 temp之后 传给temp
                    }
                }
                else if (note[i].flag == 0 && note[i].c == '+')
                {
                    if (i == 0)
                    {
                        count = count + 2;
                    }
                    else
                    {
                        // outplus << "INSERT INTO TEMP(SELECT TEMP.N1,F" << s[count] << ".N2 FROM TEMP INNER JOIN F" << s[count] << " ON TEMP.N2=F" << s[count] << ".N1);" << endl;
                        List<dint> din = new List<dint>();
                        dint n = new dint();
                        n.a = 1;
                        n.b = 0;
                        din.Add(n);
                        if (s[count] == 'a')
                        {
                            Table tablejoin = TEMP.innerJoin(Fa, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fa.N2", null) }, null));
                        }
                        else if (s[count] == 'b')
                        {
                            Table tablejoin = TEMP.innerJoin(Fb, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fb.N2", null) }, null));
                        }
                        else if (s[count] == 'c')
                        {
                            Table tablejoin = TEMP.innerJoin(Fc, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fc.N2", null) }, null));
                        }
                        else if (s[count] == 'd')
                        {
                            Table tablejoin = TEMP.innerJoin(Fd, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fd.N2", null) }, null));
                        }
                        else if (s[count] == 'e')
                        {
                            Table tablejoin = TEMP.innerJoin(Fe, "tablejoin", din);
                            TEMP.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("Fe.N2", null) }, null));
                        }
                        count = count + 2;
                    }
                }
                else if (note[i].flag != 0)
                {
                    for (int j = 0; j < note[i].flag; j++)
                    {
                        if (i == 0 && j == 0)
                        {
                            count++;
                        }
                        else if (count == 2 && s[1] == '*')
                        {
                            List<dint> din = new List<dint>();
                            dint n = new dint();
                            n.a = 1;
                            n.b = 0;
                            din.Add(n);
                            // outplus << "CREATE TABLE TM(N1 int,N2 int);" << endl;
                            Database.getCurrentDatabase().createTable("TM", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                            Table TM = new Table("TM");
                            // outplus << "INSERT INTO TM (SELECT TEMP.N1,table_" << s[count] << ".N2" << " FROM TEMP INNER JOIN table_" << s[count] << " ON TEMP.N2=table_" << s[count] << ".N1);" << endl;
                            if (s[count] == 'a')
                            {
                                Table tablejoin = TEMP.innerJoin(tablea, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablea.N2", null) }, null));
                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "INSERT INTO TEMP(SELECT * FROM table_" << s[count] << ");" << endl;
                                TEMP.insert(sname, tablea.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'b')
                            {
                                Table tablejoin = TEMP.innerJoin(tableb, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tableb.N2", null) }, null));
                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "INSERT INTO TEMP(SELECT * FROM table_" << s[count] << ");" << endl;
                                TEMP.insert(sname, tableb.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'c')
                            {
                                Table tablejoin = TEMP.innerJoin(tablec, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablec.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "INSERT INTO TEMP(SELECT * FROM table_" << s[count] << ");" << endl;
                                TEMP.insert(sname, tablec.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'd')
                            {
                                Table tablejoin = TEMP.innerJoin(tabled, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tabled.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "INSERT INTO TEMP(SELECT * FROM table_" << s[count] << ");" << endl;
                                TEMP.insert(sname, tabled.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'e')
                            {
                                Table tablejoin = TEMP.innerJoin(tablee, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablee.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "INSERT INTO TEMP(SELECT * FROM table_" << s[count] << ");" << endl;
                                TEMP.insert(sname, tablee.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));
                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }

                            count++;
                        }
                        else
                        {
                            List<dint> din = new List<dint>();
                            dint n = new dint();
                            n.a = 1;
                            n.b = 0;
                            din.Add(n);
                            // outplus << "CREATE TABLE TM(N1 int,N2 int);" << endl;
                            Database.getCurrentDatabase().createTable("TM", null, new Tuple<int, string, object>(FieldType.INTEGER, "N1", 0), new Tuple<int, string, object>(FieldType.INTEGER, "N2", 0));
                            Table TM = new Table("TM");
                            // outplus << "INSERT INTO TM (SELECT TEMP.N1,table_" << s[count] << ".N2" << " FROM TEMP INNER JOIN table_" << s[count] << " ON TEMP.N2=table_" << s[count] << ".N1);" << endl;
                            if (s[count] == 'a')
                            {
                                Table tablejoin = TEMP.innerJoin(tablea, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablea.N2", null) }, null));
                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));

                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'b')
                            {
                                Table tablejoin = TEMP.innerJoin(tableb, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tableb.N2", null) }, null));
                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));

                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'c')
                            {
                                Table tablejoin = TEMP.innerJoin(tablec, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablec.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));

                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'd')
                            {
                                Table tablejoin = TEMP.innerJoin(tabled, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tabled.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));

                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            else if (s[count] == 'e')
                            {
                                Table tablejoin = TEMP.innerJoin(tablee, "tablejoin", din);
                                TM.insert(sname, tablejoin.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("tablee.N2", null) }, null));

                                // outplus << "DROP TABLE TEMP;" << endl;
                                // outplus << "CREATE TABLE TEMP(N1 int,N2 int);" << endl;
                                TEMP.delete(null);

                                // outplus << "INSERT INTO TEMP(SELECT * FROM TM);" << endl;
                                TEMP.insert(sname, TM.select(new Tuple<string, string>[] { new Tuple<string, string>("*", null) }, null));

                                // outplus << "DROP TABLE TM;" << endl;
                                Database.getCurrentDatabase().dropTable("TM");
                            }
                            count++;
                        }
                    }
                }
            }
            // outplus << "INSERT INTO FINAL(SELECT DISTINCT TEMP.N1,TEMP.N2 FROM TEMP);" << endl;
            // outplus << "DROP TABLE TEMP;" << endl;
            // outplus << "DROP TABLE TP;" << endl;
            FINAL.insert(sname, TEMP.select(new Tuple<string, string>[] { new Tuple<string, string>("TEMP.N1", null), new Tuple<string, string>("TEMP.N2", null) }, null));
            Database.getCurrentDatabase().dropTable("TEMP");
            Database.getCurrentDatabase().dropTable("TP");
            //FINAL.printTable();
            List<List<long>> tempResult = FINAL.getCellIds();
            List<List<Element>> eleList = new List<List<Element>>();
            result.Add(tempResult[0]);
            for (int i = 0; i < tempResult.Count; i++)
            {
                if (Table.elementIndex(result, tempResult[i]) == -1)
                {
                    result.Add(tempResult[i]);
                }
            }
            Table.printEle(result);

        }
    }
}
