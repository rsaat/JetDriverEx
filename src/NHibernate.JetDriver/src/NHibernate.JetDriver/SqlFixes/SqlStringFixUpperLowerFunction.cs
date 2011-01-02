﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using System.Text.RegularExpressions;

namespace NHibernate.JetDriver.SqlFixes
{
    /// <summary>Change "upper" to "ucase" and "lower" to "lcase"
    ///     example test : ExtraLazy   Get 
    ///  </summary>
    public class SqlStringFixUpperLowerFunction : SqlStringFix
    {
        private static Regex _regexLocate = new Regex(
                 "(upper|lower)\\s*\\((.+?)\\)",
             RegexOptions.IgnoreCase
             | RegexOptions.Singleline
             | RegexOptions.CultureInvariant
             );

        public override string FixSql(string sql)
        {
            if (!_regexLocate.IsMatch(sql))
            {
                return sql;
            }

            var matches = _regexLocate.Matches(sql);

            foreach (Match match in matches)
            {
                var sqlToReplace = match.Value;
                sqlToReplace = sqlToReplace.Replace("upper", "ucase");
                sqlToReplace = sqlToReplace.Replace("lower", "lcase");
                sql = sql.Replace(match.Value, sqlToReplace);
            }

            return sql;

        }
    }
}
