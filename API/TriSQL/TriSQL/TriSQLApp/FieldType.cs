using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;

namespace TriSQLApp {
    /// <summary>
    /// 定义了各个类型的宏
    /// </summary>
    class FieldType {
        /// <summary>
        /// 字符串
        /// </summary>
        public const int STRING = 1;

        /// <summary>
        /// 整数
        /// </summary>
        public const int INTEGER = 2;

        /// <summary>
        /// 浮点数
        /// </summary>
        public const int DOUBLE = 3;

        /// <summary>
        /// 日期类型 yyyy-MM-dd 格式
        /// </summary>
        public const int DATETIME = 4;

        /// <summary>
        /// 长整数
        /// </summary>
        public const int LONG = 5;

        /// <summary>
        /// 根据Element对象以及其类型，提取其值
        /// </summary>
        /// <param name="ele">Element对象</param>
        /// <param name="type">所存数据类型</param>
        /// <returns>所存的值</returns>
        public static object getValue(Element ele, int type) {
            Object value = null;
            if (ele.Equals(null))
            {
                throw new NullReferenceException("Element不可为空");
            }
            if (type < STRING || type > LONG)
            {
                throw new Exception(String.Format("不存在的字段类型:{0}", type));
            }
            else
            {
                switch (type)
                {
                    case INTEGER:
                        value = Convert.ToInt32(ele.intField);
                        break;
                    case STRING:
                        value = Convert.ToString(ele.stringField);
                        break;
                    case LONG:
                        value = Convert.ToInt64(ele.longField);
                        break;
                    case DATETIME:
                        value = Convert.ToDateTime(ele.dateField);
                        break;
                    case DOUBLE:
                        value = Convert.ToDouble(ele.doubleField);
                        break;
                    default:
                        throw new Exception(String.Format("不存在的字段类型:{0}", type));
                }
            }
            return value;
        }

        public static List<Object> getValues(List<Element> eles, List<int> types)
        {
            List<Object> values = new List<object>();
            foreach(Element ele in eles)
            {
                values.Add(getValue(ele, types[eles.IndexOf(ele)]));
            }
            return values;
        }

        /// <summary>
        /// 根据类型及值，存入element
        /// </summary>
        /// <param name="value">值</param>
        /// <param name="type">类型</param>
        /// <returns>存入的element</returns>
        public static Element setValue(object value, int type)
        {
            Element ele = new Element();
            if (value == null)  //无默认值
            {
                if (type < STRING || type > LONG) {
                    throw new Exception(String.Format("不存在的字段类型:{0}", type));
                }
                return null;
            } else {
                switch (type)
                {
                    case INTEGER:
                        ele.intField = Convert.ToInt32(value);
                        break;
                    case STRING:
                        ele.stringField = Convert.ToString(value);
                        break;
                    case LONG:
                        ele.longField = Convert.ToInt64(value);
                        break;
                    case DATETIME:
                        ele.dateField = Convert.ToDateTime(value);
                        break;
                    case DOUBLE:
                        ele.doubleField = Convert.ToDouble(value);
                        break;
                    default:
                        throw new Exception(String.Format("不存在的字段类型:{0}", type));
                }
            }
            return ele;
        }
    }
}
