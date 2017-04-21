using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TriModel;

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
        public List<ConditionPair> conditions;
        public Stack<LogicalOperand> logicOptStack;

        //初始构造函数
        public Condition(Table table, string condition)
        {
            this.conditions = new List<ConditionPair>();
            this.logicOptStack = new Stack<LogicalOperand>();

            this.table = table;
            string[] strs = condition.Split(' ');

            for (int i = 0; i < strs.Length; i = i + 4)
            {
                Console.WriteLine(strs[i] + " " + strs[i + 1] + " " + strs[i + 2] + " " + strs[i + 3]);
                string param1 = strs[i];
                string param2 = strs[i + 1];
                string param3 = strs[i + 2].TrimStart('\"').TrimEnd('\"');
                string param4 = strs[i + 3];
                Operand opt = Condition.getOperand(param2);
                RightOptType type =  Condition.getType(param3, opt);

                this.conditions.Add(new ConditionPair(param1, opt, type, type==RightOptType.CONSTANT?convertType(param1,param3):param3));
                
                if (!param4.Equals("#"))
                {
                    this.logicOptStack.Push(Condition.getLogicOpt(param4));
                }
            }

            this.logicOptStack.Reverse();
        }

        public Condition(Table table, string condition, List<Table> nestedTable)
        {
            this.conditions = new List<ConditionPair>();
            this.logicOptStack = new Stack<LogicalOperand>();

            this.table = table;
            string[] strs = condition.Split(' ');
            int nestedIndex = 0;

            for (int i = 0; i < strs.Length; i = i + 4)
            {
                Console.WriteLine(strs[i] + " " + strs[i + 1] + " " + strs[i + 2] + " " + strs[i + 3]);
                string param1 = strs[i];
                string param2 = strs[i + 1];
                string param3 = strs[i + 2].TrimStart('\"').TrimEnd('\"');
                string param4 = strs[i + 3];
                Operand opt = Condition.getOperand(param2);
                RightOptType type = Condition.getType(param3, opt);

                if (opt == Operand.IN)
                {
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
                else
                {
                    this.conditions.Add(new ConditionPair(param1, opt, type, type == RightOptType.CONSTANT ? convertType(param1, param3) : param3));
                }
                
                if (!param4.Equals("#"))
                {
                    this.logicOptStack.Push(Condition.getLogicOpt(param4));
                }
            }

            this.logicOptStack.Reverse();
        }

        public Condition(Table table, List<ConditionPair> conditions, Stack<LogicalOperand> logicOptStack)
        {
            this.table = table;
            this.conditions = conditions;
            this.logicOptStack = logicOptStack;
            if (conditions.Count != logicOptStack.Count + 1)
            {
                throw new Exception("逻辑操作符数量与条件式数量不匹配");
            }
        }

        public bool getResult(List<object> rowData)
        {
            List<ConditionPair> curConditon = this.conditions;
            ConditionPair cp;
            Stack<bool> cmpResultStack = new Stack<bool>();
            int rowIndex = 0;
            bool isMatch = false;

            //先遍历where条件对
            for (int i = 0; i < curConditon.Count; i++)
            {
                isMatch = false;
                cp = curConditon[i];
                object leftValue = rowData[rowIndex++];

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
                    cmpResultStack.Push(isMatch);
                        break;
                }

                object rightValue = null;
                if (cp.rightValType == RightOptType.CONSTANT)
                {
                    rightValue = cp.rightValue;
                }
                else if (cp.rightValType == RightOptType.FIELD) 
                {
                    rightValue = rowData[rowIndex++];
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
                cmpResultStack.Push(isMatch);
            }
            cmpResultStack.Reverse();

            if (logicOptStack.Count + 1 != cmpResultStack.Count)
            {
                throw new Exception("逻辑操作符数量与条件式数量不匹配");
            }

            // 逻辑运算
            while (logicOptStack.Count > 0)
            {
                bool cmpResult1 = cmpResultStack.Peek();
                cmpResultStack.Pop();
                bool cmpResult2 = cmpResultStack.Peek();
                cmpResultStack.Pop();

                if (logicOptStack.Peek() == LogicalOperand.AND)
                {
                    cmpResultStack.Push(cmpResult1 && cmpResult2);
                }
                else if (logicOptStack.Peek() == LogicalOperand.OR)
                {
                    cmpResultStack.Push(cmpResult1 || cmpResult2);
                }
                else
                {
                    cmpResultStack.Push(false);
                    throw new Exception("暂不支持此逻辑运算");
                }
                logicOptStack.Pop();  
            }

            return cmpResultStack.Peek();
        }

        public object convertType(string fieldName, string param)
        {
            int index = table.columnNames.IndexOf(fieldName);
            int type = table.columnTypes[index];
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

        public static Operand getOperand(string opt)
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

        public static RightOptType getType(string type, Operand opt)
        {
            string[] strs = type.Split('.');
            if (strs.Length > 1)
            {
                return RightOptType.FIELD;
            }
            else if (strs.Length ==1 && opt == Operand.IN)
            {
                return RightOptType.TABLE;
            }
            else
            {
                return RightOptType.CONSTANT;
            }
        }

        public static LogicalOperand getLogicOpt(string opt)
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
