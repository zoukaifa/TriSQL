using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;

namespace TriSQLApp
{
    /// <summary>
    /// 接口使用说明：
    /// 初始化查询字符串：注意括号左右两侧应有空格, 字符串类型的数据应该用\"string\"的形式, 嵌套形式in后面的字符串内容随便
    /// string cond = "( Stu.name == mike or Stu.name == cindy ) and Stu.id <= Course.sid and val in tmp"
    /// 初始化Condition对象：table为主查询表对象, cond为查询字符串, tableList为嵌套查询的表列表
    /// Condition condition = new Condition(Table table, string cond, List<Table> tableList);
    /// 调用getResult()接口获取判断结果,其中rowData输入顺序和查询字符串保持一致, 重复属性无需重复输入
    /// condition.getResult(rowData));
    /// </summary>

    public enum Operand
    {
        EQUAL,
        NOT_EQUAL,
        GREATER,
        LESS,
        GREATER_EQUAL,
        LESS_EQAUL,
        IN
    };

    public enum LogicalOperand
    {
        AND,
        OR,
        NOT,
    };

    public enum RightOptType
    {
        CONSTANT,
        FIELD,
        TABLE
    }

    /// <summary>
    /// 条件对
    /// </summary>
    class ConditionPair
    {
        public string left;
        public Operand operand;
        public RightOptType rightValType;
        public object rightValue;

        public Table rightTable;               //保存in右部的table对象
        public List<object> rightValueList;    //根据左部的指定列名获取table对象中某列数据

        public ConditionPair()
        {
            Console.WriteLine("无参数初始化构造函数构造成功");
        }

        public ConditionPair(string left, Operand operand, RightOptType rightIsValue, object rightValue)
        {
            Console.WriteLine("带参数初始化构造函数构造成功(无嵌套)");
            this.left = left;
            this.operand = operand;
            this.rightValType = rightIsValue;
            this.rightValue = rightValue;
        }

        public ConditionPair(string left, Operand operand, RightOptType rightIsValue, Table table)
        {
            Console.WriteLine("带参数初始化构造函数构造成功(有嵌套)");
            this.left = left;
            this.operand = operand;
            this.rightValType = rightIsValue;

            this.rightTable = table;
            this.rightValueList = table.getColumn(left); //需增加一个异常捕获
        }

        //public ConditionPair(string left, Operand operand, RightOptType rightIsValue, List<Table> nestedTable)
        //{
        //    Console.WriteLine("带参数(嵌套)初始化构造函数构造成功");
        //    this.left = left;
        //    this.operand = operand;
        //    this.rightValType = rightIsValue;
        //    this.rightTableList = nestedTable;
        //}
    }

    /// <summary>
    /// where子句(由一系列的条件对及逻辑符组成)
    /// </summary>
    class Condition
    {
        private Table table;
        private List<ConditionPair> conditions;
        //private Dictionary<string, int> columnName;
        private string[] strs;
        private int isDefault = -1;

        //初始构造函数
        public Condition(Table table, string condition, List<Table> nestedTable = null)
        {
            Console.WriteLine("条件字符串" + condition);
            if (condition == null || condition.Equals(""))
            {
                this.isDefault = 0;
            }
            else
            {
                this.isDefault = 1;
                this.conditions = new List<ConditionPair>();
                //this.columnName = new Dictionary<string, int>();

                this.table = table;
                this.strs = condition.Split(' ');
                //int columnIndex = 0;
                int nestedIndex = 0;

                for (int i = 0; i < strs.Length; i++)
                {
                    if (strs[i].Equals("in"))
                    {
                        //Console.WriteLine(strs[i - 1] + " " + strs[i] + " " + strs[i + 1]);
                        string param1 = strs[i - 1];
                        string param2 = strs[i];
                        string param3 = strs[i + 1].TrimStart('\"').TrimEnd('\"');

                        Operand opt = Condition.getOperand(param2);
                        RightOptType type = Condition.getType(param3, opt);

                        if (nestedIndex < nestedTable.Count)
                        {
                            this.conditions.Add(new ConditionPair(param1, opt, type, nestedTable[nestedIndex]));
                            nestedIndex++;
                            //if (!this.columnName.ContainsKey(param1))
                            //{
                            //    this.columnName.Add(param1, columnIndex++);
                            //}
                        }
                        else
                        {
                            throw new Exception("嵌套Table数量不正确");
                        }
                        strs[i - 1] = " ";
                        strs[i] = "?";
                        strs[i + 1] = " ";
                    }
                    else if (strs[i].Equals("==") || strs[i].Equals("!=") || strs[i].Equals(">") || strs[i].Equals(">=") || strs[i].Equals("<")
                        || strs[i].Equals("<="))
                    {
                        Console.WriteLine(strs[i - 1] + " " + strs[i] + " " + strs[i + 1]);
                        string param1 = strs[i - 1];
                        string param2 = strs[i];
                        string param3 = strs[i + 1].TrimStart('\"').TrimEnd('\"');

                        Operand opt = Condition.getOperand(param2);
                        RightOptType type = Condition.getType(param3, opt);

                        this.conditions.Add(new ConditionPair(param1, opt, type, type == RightOptType.CONSTANT ? convertType(param1, param3) : param3));
                        //if (!this.columnName.ContainsKey(param1))
                        //{
                        //    this.columnName.Add(param1, columnIndex++);
                        //}

                        //Console.WriteLine(type);
                        //if (type.Equals(RightOptType.FIELD))
                        //{
                        //    this.columnName.Add(param3, columnIndex++);
                        //}

                        strs[i - 1] = " ";
                        strs[i] = "?";
                        strs[i + 1] = " ";
                    }
                    else if (strs[i].Equals("or"))
                    {
                        strs[i] = "+";
                    }
                    else if (strs[i].Equals("and"))
                    {
                        strs[i] = "*";
                    }
                }
            }

        }

        /// <summary>
        /// where子句判断接口
        /// </summary>
        /// <param name="rowData"></param>
        /// <returns></returns>
        public bool getResult(List<object> rowData)
        {
            Console.WriteLine("行数据：");
            for (int i = 0; i < rowData.Count; i++ )
            {
                Console.WriteLine(rowData[i]);
            }
            if (this.isDefault == -1)
            {
                Console.WriteLine("尚未初始化条件");
                return true;
            }
            else if (this.isDefault == 0)
            {
                Console.WriteLine("默认查询表全部内容");
                return true;
            }

            List<ConditionPair> curConditon = this.conditions;
            ConditionPair cp;
            List<bool> cmpResult = new List<bool>();
            //int rowIndex = 0;
            bool isMatch = false;

            //先遍历where条件对
            for (int i = 0; i < curConditon.Count; i++)
            {
                isMatch = false;
                cp = curConditon[i];
                // 修改左值获取方式（2017/5/4）
                object leftValue = rowData[table.getColumnNames().IndexOf(cp.left)];
                //object leftValue = rowData[this.columnName[cp.left]];
                Console.WriteLine(leftValue);

                //处理嵌套的情况
                if (cp.operand == Operand.IN && cp.rightValType == RightOptType.TABLE)
                {
                    for (int j = 0; j < cp.rightValueList.Count; j++)
                    {
                        if (leftValue.Equals(cp.rightValueList[j]))
                        {
                            isMatch = true;
                            break;
                        }
                    }
                    cmpResult.Add(isMatch);
                    break;
                }

                object rightValue = null;
                if (cp.rightValType == RightOptType.CONSTANT)
                {
                    Console.WriteLine("常量值比较");
                    rightValue = cp.rightValue;
                }
                else if (cp.rightValType == RightOptType.FIELD)
                {
                    //rightValue = rowData[this.columnName[cp.rightValue.ToString()]];
                    //修改右值获取方式
                    rightValue = rowData[table.getColumnNames().IndexOf(cp.rightValue.ToString())];
                }
                else
                {
                    throw new Exception("右操作数类型异常");
                }

                // 判断类型是否匹配
                if (leftValue.GetType() != rightValue.GetType())
                {
                    //Console.WriteLine(leftValue.GetType() + "," + rightValue.GetType());
                    throw new Exception("类型不匹配");
                }

                // 根据指定操作符进行运算
                switch (cp.operand)
                {
                    case Operand.EQUAL:
                        isMatch = leftValue.Equals(rightValue) ? true : false;
                        break;
                    case Operand.NOT_EQUAL:
                        isMatch = leftValue.Equals(rightValue) ? false : true;
                        break;
                    case Operand.GREATER:
                        if (leftValue is int || leftValue is long || leftValue is double || leftValue is DateTime)
                        {
                            if (leftValue is int)
                            {
                                isMatch = (int)leftValue > (int)rightValue ? true : false;
                            }
                            else if (leftValue is long)
                            {
                                isMatch = (long)leftValue > (long)rightValue ? true : false;
                            }
                            else if (leftValue is double)
                            {
                                isMatch = (double)leftValue > (double)rightValue ? true : false;
                            }
                            else
                            {
                                Console.WriteLine("日期比较还木有做");
                            }
                        }
                        else
                        {
                            Console.WriteLine("大于操作暂不支持该数据类型");
                            //throw new Exception("大于操作暂不支持该数据类型");
                        }
                        break;
                    case Operand.GREATER_EQUAL:
                        if (leftValue is int || leftValue is long || leftValue is double || leftValue is DateTime)
                        {
                            if (leftValue is int)
                            {
                                isMatch = (int)leftValue >= (int)rightValue ? true : false;
                            }
                            else if (leftValue is long)
                            {
                                isMatch = (long)leftValue >= (long)rightValue ? true : false;
                            }
                            else if (leftValue is double)
                            {
                                isMatch = (double)leftValue >= (double)rightValue ? true : false;
                            }
                            else
                            {
                                Console.WriteLine("日期比较还木有做");
                            }
                        }
                        else
                        {
                            throw new Exception("大于或等于操作暂不支持该数据类型");
                        }
                        break;
                    case Operand.LESS:
                        if (leftValue is int || leftValue is long || leftValue is double || leftValue is DateTime)
                        {
                            if (leftValue is int)
                            {
                                isMatch = (int)leftValue < (int)rightValue ? true : false;
                            }
                            else if (leftValue is long)
                            {
                                isMatch = (long)leftValue < (long)rightValue ? true : false;
                            }
                            else if (leftValue is double)
                            {
                                isMatch = (double)leftValue < (double)rightValue ? true : false;
                            }
                            else
                            {
                                Console.WriteLine("日期比较还木有做");
                            }
                        }
                        else
                        {
                            throw new Exception("小于操作暂不支持该数据类型");
                        }
                        break;
                    case Operand.LESS_EQAUL:
                        if (leftValue is int || leftValue is long || leftValue is double || leftValue is DateTime)
                        {
                            if (leftValue is int)
                            {
                                isMatch = (int)leftValue <= (int)rightValue ? true : false;
                            }
                            else if (leftValue is long)
                            {
                                isMatch = (long)leftValue <= (long)rightValue ? true : false;
                            }
                            else if (leftValue is double)
                            {
                                isMatch = (double)leftValue <= (double)rightValue ? true : false;
                            }
                            else
                            {
                                Console.WriteLine("日期比较还木有做");
                            }
                        }
                        else
                        {
                            throw new Exception("小于操作暂不支持该数据类型");
                        }
                        break;
                    default:
                        throw new Exception("无效操作符" + cp.operand);
                }
                Console.WriteLine(isMatch + ", " + leftValue.GetType() + ", " + rightValue.GetType());
                cmpResult.Add(isMatch);
            }

            int count = 0;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < this.strs.Length; i++)
            {
                if (this.strs[i].Equals("?"))
                {
                    this.strs[i] = cmpResult[count++] == true ? "1" : "0";
                }
                sb.Append(strs[i]);
            }

            Console.WriteLine(sb.ToString());
            DataTable dt = new DataTable();
            int result = (int)dt.Compute(sb.ToString(), "");

            return result > 0 ? true : false;
        }

        //类的其它内部方法
        private object convertType(string fieldName, string param)
        {
            int index = table.getColumnNames().IndexOf(fieldName);
            int type = table.getColumnTypes()[index];
            object result = null;
            switch (type)
            {
                case 1:
                    result = param;
                    break;
                case 2:
                    result = Convert.ToInt32(param);
                    break;
                case 3:
                    result = Convert.ToDouble(param);
                    break;
                case 5:
                    result = Convert.ToInt64(param);
                    break;
                case 4:
                    result = Convert.ToDateTime(param);
                    break;
                default:
                    throw new Exception("数据类型异常");
            }
            return result;
        }

        private static Operand getOperand(string opt)
        {
            if (opt.Equals("=="))
            {
                return Operand.EQUAL;
            }
            else if (opt.Equals("!="))
            {
                return Operand.NOT_EQUAL;
            }
            else if (opt.Equals(">"))
            {
                return Operand.GREATER;
            }
            else if (opt.Equals(">="))
            {
                return Operand.GREATER_EQUAL;
            }
            else if (opt.Equals("<"))
            {
                return Operand.LESS;
            }
            else if (opt.Equals("<="))
            {
                return Operand.LESS_EQAUL;
            }
            else
            {
                return Operand.IN;
            }
        }

        private static RightOptType getType(string type, Operand opt)
        {
            string[] strs = type.Split('.');
            if (strs.Length > 1)
            {
                return RightOptType.FIELD;
            }
            else if (strs.Length == 1 && opt == Operand.IN)
            {
                return RightOptType.TABLE;
            }
            else
            {
                return RightOptType.CONSTANT;
            }
        }

        private static LogicalOperand getLogicOpt(string opt)
        {
            if (opt.Equals("and"))
            {
                return LogicalOperand.AND;
            }
            else if (opt.Equals("or"))
            {
                return LogicalOperand.OR;
            }
            else
            {
                return LogicalOperand.NOT;
            }
        }


    }
}