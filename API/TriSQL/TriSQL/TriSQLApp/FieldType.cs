using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TriSQLApp {
    /// <summary>
    /// 定义了各个类型的宏
    /// </summary>
    class FieldType {
        /// <summary>
        /// 字符串
        /// </summary>
        public static int STRING = 1;

        /// <summary>
        /// 整数
        /// </summary>
        public static int INTEGER = 2;

        /// <summary>
        /// 浮点数
        /// </summary>
        public static int DOUBLE = 3;

        /// <summary>
        /// 日期类型 yyyy-MM-dd 格式
        /// </summary>
        public static int DATETIME = 4;

        /// <summary>
        /// 长整数
        /// </summary>
        public static int LONG = 5;

        /// <summary>
        /// CellId类型
        /// </summary>
        public static int CELLID = 6;
    }
}
