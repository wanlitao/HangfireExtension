using System;
using System.Globalization;

namespace Hangfire.SQLite.Common
{
    /// <summary>
    /// 类型转换助手
    /// </summary>
    public class TypeConvertHelper
    {
        /// <summary>
        /// 解析 数据库值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static T ParseDbValue<T>(object value)
        {
            if (value == null || value is DBNull) return default(T);
            if (value is T) return (T)value;
            var type = typeof(T);
            type = Nullable.GetUnderlyingType(type) ?? type;
            if (type.IsEnum)
            {
                if (value is float || value is double || value is decimal)
                {
                    value = Convert.ChangeType(value, Enum.GetUnderlyingType(type), CultureInfo.InvariantCulture);
                }
                return (T)Enum.ToObject(type, value);
            }            
            return (T)Convert.ChangeType(value, type, CultureInfo.InvariantCulture);
        }
    }
}
