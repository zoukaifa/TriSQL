#include <iostream>
#include <fstream>
#include <map>
#include <string>
#include <queue>
#include <Windows.h>
#include <stdlib.h>
#include<sstream>

#include "mysql_connection.h"

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>

using namespace std;

class str2
{
public:
	string s1;
	string s2;
};
struct dint {
	int a, b;
};
unsigned int MAX(int a, int b)
{
	return (a > b) ? a : b;
}
bool comp(dint a, dint b) {
	return a.b < b.b;
}
int create() {
	ifstream in("C:\\Users\\lining\\Desktop\\graph.txt");
	if (!in.is_open()) {
		cout << "error" << endl;
		exit(1);
	}
	int n;
	in >> n; 
	map<string, int> M;
	vector<str2> Q;
	int g = 0;
	for (int i = 0; i < n; i++) {

		string s1, s2;
		in >> s1 >> s2;
		str2 s;
		s.s1 = s1;
		s.s2 = s2;
		Q.push_back(s);
		if (M.find(s1) == M.end()) {
			M[s1] = g++;
		}
		if (M.find(s2) == M.end()) {
			M[s2] = g++;
		}
	}
	in.close();
	ofstream out("C:\\Users\\lining\\Desktop\\SQLcreategraph.txt");
	for (map<string, int>::iterator  i = M.begin(); i != M.end(); i++) {
		out << "INSERT INTO label VALUES(" << i->second + 1 << ",'" << i->first.substr(0,1) << "');" << endl;
	}
	for (vector<str2>::iterator i = Q.begin(); i != Q.end(); i++) {
		out << "INSERT INTO graph VALUES(" << M[(*i).s1]+1 << "," << M[(*i).s2]+1 << ");" << endl;
		out << "INSERT INTO graph VALUES(" << M[(*i).s2] + 1 << "," << M[(*i).s1] + 1 << ");" << endl;
	}
	out.close();
	return 0;
}
void int2str(const int &int_temp, string &string_temp)
{
	stringstream stream;
	stream << int_temp;
	string_temp = stream.str();   //此处也可以用 stream>>string_temp  
}
int query(void) {
	string q1[4];
	q1[0] = "b";
	q1[1] = "a";
	q1[2] = "e";
	q1[3] = "c";
	string q2[3];
	q2[0] = "e";
	q2[1] = "a";
	q2[2] = "d";
	string q3[2];
	q3[0] = "c";
	q3[1] = "d";
	const int querynum = 3;
	int num[querynum] = { 4,3,2 };

	string* Q[3] = { q1,q2,q3 };
	ofstream out("C:\\Users\\lining\\Desktop\\SQLquery.txt");
	try {
		sql::Driver *driver;
		sql::Connection *con;
		sql::Statement *stmt;
		sql::ResultSet *res;
		sql::PreparedStatement *pstmt;
		/* Create a connection */
		//driver = get_driver_instance();
		//con = driver->connect("tcp://127.0.0.1:3306", "root", "1234");
		/* Connect to the MySQL graphengine database */
		//con->setSchema("graphengine");
		//stmt = con->createStatement();
/*		stmt->execute(
		"drop table if exists ttt;\
		create table ttt(\
			node1 int,\
			node2 int,\
			label varchar(10)\
		);\
		drop table if exists q1;\
		drop table if exists q2;\
		drop table if exists q3;\
		create table q1(\
			b int,\
			a int,\
			e int,\
			c int\
		);\
		create table q2(\
			e int,\
			a int,\
			b int\
		);\
		create table q3(\
			c int,\
			d int\
		); ");
		//delete stmt;
		/* '?' is the supported placeholder syntax */
		out << "drop table if exists ttt;\
		create table ttt(\
			node1 int,\
			node2 int,\
			label varchar(10)\
		);" << endl;
		for (int i = 0; i<querynum; i++){
			string sa =
				"insert into ttt\
					select *\
					from(select node1, node2, label\
						from(select graph.node1, graph.node2\
							from(select node\
								from label\
				where label = '";
			sa += Q[i][0];
			sa += "'\
					)as ta\
							inner join graph\
					on ta.node = graph.node1\
					)as tb\
						inner join label\
					on tb.node2 = label.node order by node\
					)as tc\
				where tc.label in(";
			string sb;
			for (int j = 1; j < num[i]; j++) {
				sb += "'";
				sb += Q[i][j];
				sb += "'";
				if (j != num[i] -1 ) sb += ",";
				else sb += ");";
			}
			out << sa + sb << endl;
			//pstmt = con->prepareStatement(sa+sb);
			//pstmt->setString(1, Q[i][0]);
			//pstmt->executeQuery();
			string si;
			int2str(i+1, si);
			out << "drop table if exists q";
			out << si << ";" << endl;
			out << "create table q" << si << "(";
			for (int j = 0; j < num[i]; j++) {
				out << Q[i][j] << " ";
				if (j != num[i] - 1)out << "int,";
				else out << "int);" << endl;
			}
			string sc = "insert into q";
			sc += si;
			if (num[i] == 2) sc += " select node1, node2 from ttt;";
			else {
				sc += " select ja.node1,";
				char a = 'a';
				for (int j = 1; j < num[i]; j++) {
					sc += "j";
					sc += a;
					if (j != num[i] - 1)sc += ".node2,";
					else sc += ".node2 ";
					a += 1;
				}
				sc += "from";
				a = 'a';
				for (int j = 1; j < num[i]; j++) {
					sc += "(select node1, node2\
					from ttt\
					where label = '";
					sc += Q[i][j];
					sc += "')as j";
					sc += a;

					if (j != 1)
					{
						sc += " on ";
						sc += "ja.node1 = j";
						sc += a;
						if (j != num[i] - 1)sc += ".node1";
						else sc += ".node1;";
					}

					if (j != num[i] - 1)sc += " inner join ";
					else sc += " ";
					a += 1;
				}
			}
			out << sc << endl;
			out << "delete from ttt;" << endl;	
		}
		//delete pstmt;
		


	}catch (sql::SQLException &e) {
		cout << "# ERR: SQLException in " << __FILE__;
		cout << "(" << __FUNCTION__ << ") on line "
			<< __LINE__ << endl;
		cout << "# ERR: " << e.what();
		cout << " (MySQL error code: " << e.getErrorCode();
		cout << ", SQLState: " << e.getSQLState() <<
			" )" << endl;
	}

	for (int z = 0; z < querynum - 1; z++) {//不断更新num[0],Q[0],q1
		int equalsize = 0;
		dint* equal = 0;
		int stepa = num[0];
		int stepb = num[z + 1];
		string *qa, *qb;
		qa = Q[0];
		qb = Q[z + 1];
		equal = (dint*)malloc(sizeof(dint)*MAX(stepa, stepb));
		/*pre process*/
		for (int i = 0; i < stepa; i++) {
			for (int j = 0; j < stepb; j++) {
				if (!qa[i].compare(qb[j])) {
					equal[equalsize].a = i;
					equal[equalsize].b = j;
					equalsize++;
				}
			}
		}
		sort(equal, equal + equalsize, comp);
		/*init result*/
		int* res;
		int stepres;
		stepres = stepa + stepb - equalsize;
		string *resQ = new string[stepres];
		int temp = 0, temp1 = 0;
		for (int k = 0; k < stepa + stepb; k++) {
			if (k < stepa) {
				resQ[k] = qa[k];
				temp = k;
			}
			else if (equal[temp1].b != k - stepa) {
				resQ[(++temp)] = qb[(k - stepa)];
			}
			else {
				temp1++;
			}
		}
		out << "drop table if exists new;create table new(";
		for (int j = 0; j < stepres; j++) {
			out << resQ[j] << " ";
			if (j != stepres - 1)out << "int,";
			else out << "int);" << endl;
		}
		out << "insert into new ";
		out << "select ";
		for (int i = 0; i < stepres; i++) {
			if (i<stepa) out << "q1.";
			else out << "q" << z + 2 << ".";
			out << resQ[i];
			if (i != stepres - 1)out << ",";
			else out << " ";
		}
		out << "from q1 inner join q";
		out << z + 2;
		out << " on ";
		for (int i = 0; i < equalsize; i++) {
			out << "q1." << qa[equal[i].a] << "=" << "q" << z + 2 << "." << qa[equal[i].a];
			if (i != equalsize - 1) out << " and ";
			else out << ";";
		}
		//free(Q[0]);
		Q[0] = resQ;
		out << endl;
		num[0] = stepres;
		free(equal);
		out << "drop table " << "q" << z + 2 << ";"<<endl;
		out << "drop table q1;" << endl;
		out << "alter table new rename to q1;" << endl;
	}

	out << "drop table ttt;" << endl;
	out.close();
	
	return 0;
}
int main() {
	string s;
	
	while (true) {
		cin >> s;
		if (s.compare("-c") == 0) {
			create();
			break;
		}
		else if (s.compare("-q")==0) {
			query();
			break;
		}
		else cout << "error try again" << endl;
	}
}