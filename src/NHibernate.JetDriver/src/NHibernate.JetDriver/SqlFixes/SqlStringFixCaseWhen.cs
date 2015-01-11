using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernate.JetDriver.SqlFixes
{
    /// <summary>Fix "Case When then else end" SQL with Access Switch. 
    ///          example test NegativeTimesheetsWithEqualsFalse   
    ///          Needs implementation 
    /// </summary>
    public class SqlStringFixCaseWhen : SqlStringFix
    {

        private  static Regex _regexCaseWhen = new Regex(
                  "case[^\\w](.*?)[^\\w]end",
                RegexOptions.IgnoreCase
                | RegexOptions.Singleline
                | RegexOptions.CultureInvariant
                );

        public override string FixSql(string sql)
        {


            return sql;	 

            if (!_regexCaseWhen.IsMatch(sql))
	        {
                return sql;	 
	        }      
            
            var matches = _regexCaseWhen.Matches(sql);

        
            foreach (Match match in matches)
	        {
                
	        }

            return sql;
            
        }
    }
}
