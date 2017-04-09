using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TriSQLApp
{
    class Condition
    {
        private Table table;
        private object content; //当该节点是叶子节点时，所存储的内容，至少提供get方法
        private int contentType; //content的类型，是下面几种宏的一种
        public static int CONSTANT; //常量
        public static int FIELD; //字段
        private Condition leftChild; //左孩子
        private Condition rightChild; //右孩子
        private Condition parent; //父节点，如果实现时没有必要，可以去掉这个属性
        private string op; //运算符（如果这个节点存在子节点的话），可以是"&","|","!","+","-","*","/","in"

        public Condition(Table table, object content, int contentType)
        {

        }

        public static Condition operator &(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator |(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator +(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator -(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator *(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator /(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator >(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator <(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator ==(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator !=(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator >=(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator <=(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public static Condition operator !(Condition con)
        {
            return null;
        }

        public static Condition inTable(Condition leftCon, Condition rightCon)
        {
            return null;
        }

        public object getResult(List<List<object>> rows)
        {
            return null;
        }
    }
}
