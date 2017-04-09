using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Trinity;
using TriSQL;

namespace TriSQLApp
{
    //以下全是测试代码
    internal class Program
    {
        static void Tes(params string[] pairs) { }
        static void Tes(string[][] pairs = null) { }
        public static Program isIn(Program A, Program B) {
            return null;
        }
        static void Main(string[] args)
        {
            Tes(pairs:new string[] { "aaa", "bbb" });
            Tes();
            object tt = "sss";
        }
    }
}