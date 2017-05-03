using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriSQL;
namespace TriSQLApp
{
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

    class ConditionPair
    {
        public string left;
        public Operand operand;
        public RightOptType rightValType;
        public object rightValue;
        public List<object> rightValueList;
        public List<Table> rightTableList;

        public ConditionPair()
        {
            Console.WriteLine("无参数初始化构造函数构造成功");
        }

        public ConditionPair(string left, Operand operand, RightOptType rightIsValue, object rightValue)
        {
            Console.WriteLine("带参数初始化构造函数构造成功");
            this.left = left;
            this.operand = operand;
            this.rightValType = rightIsValue;
            this.rightValue = rightValue;
        }

        public ConditionPair(string left, Operand operand, RightOptType rightIsValue, List<object> rightValue)
        {
            Console.WriteLine("带参数初始化构造函数构造成功");
            this.left = left;
            this.operand = operand;
            this.rightValType = rightIsValue;
            this.rightValueList = rightValue;
            Console.WriteLine(rightValueList.Count);
        }

        public ConditionPair(string left, Operand operand, RightOptType rightIsValue, List<Table> nestedTable)
        {
            Console.WriteLine("带参数(嵌套)初始化构造函数构造成功");
            this.left = left;
            this.operand = operand;
            this.rightValType = rightIsValue;
            this.rightTableList = nestedTable;
        }
    }


    class Condition
    {
        private Table table;
        private List<ConditionPair> conditions;
        private Dictionary<string, int> columnName;
        private string[] strs;

        //初始构造函数（不含嵌套）
        public Condition(Table table, string condition, List<Table> nestedTable = null)
        {
            this.conditions = new List<ConditionPair>();
            this.columnName = new Dictionary<string, int>();

            this.table = table;
            this.strs = condition.Split(' ');
            int columnIndex = 0;
            int nestedIndex = 0;

            for (int i = 0; i < strs.Length; i++)
            {
                if (strs[i].Equals("in"))
                {
                    Console.WriteLine(strs[i - 1] + " " + strs[i] + " " + strs[i + 1]);
                    string param1 = strs[i - 1];
                    string param2 = strs[i];
                    string param3 = strs[i + 1].TrimStart('\"').TrimEnd('\"');

                    Operand opt = Condition.getOperand(param2);
                    RightOptType type = Condition.getType(param3, opt);

                    if (nestedIndex < nestedTable.Count)
                    {
                        this.conditions.Add(new ConditionPair(param1, opt, type, nestedTable[nestedIndex]));
                        nestedIndex++;
                    }
                    else
                    {
                        throw new Exception("嵌套Table数量不正确");
                    }
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
                    if (!this.columnName.ContainsKey(param1))
                    {
                        this.columnName.Add(param1, columnIndex++);
                    }

                    Console.WriteLine(type);
                    if (type.Equals(RightOptType.FIELD))
                    {
                        this.columnName.Add(param3, columnIndex++);
                    }

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

        //where子句判断接口
        public bool getResult(List<object> rowData)
        {
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
                object leftValue = rowData[this.columnName[cp.left]];

                //处理嵌套的情况
                if (cp.operand == Operand.IN && cp.rightValType == RightOptType.TABLE)
                {
                    // 调用邹同学的Table.getColumn()的接口
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
                    rightValue = cp.rightValue;
                }
                else if (cp.rightValType == RightOptType.FIELD)
                {
                    rightValue = rowData[this.columnName[cp.rightValue.ToString()]];
                }
                else
                {
                    throw new Exception("右操作数类型异常");
                }

                // 判断类型是否匹配
                if (leftValue.GetType() != rightValue.GetType())
                {
                    Console.WriteLine(leftValue.GetType() + "," + rightValue.GetType());
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