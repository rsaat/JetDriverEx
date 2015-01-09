using System;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using NHibernate.Dialect.Function;
using System.Text.RegularExpressions;
using NHibernate.Dialect.Schema;
using NHibernate.JetDriver.Schema;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using NHibernate.Engine;

namespace NHibernate.JetDriver
{

   public class JetDialectEx:JetDialect
    {
       public JetDialectEx():base()
       {
           RegisterFunction("concat", new VarArgsSQLFunction(NHibernateUtil.String, "(", "+", ")"));
           RegisterFunction("length", new StandardSQLFunction("len", NHibernateUtil.Int32));
           RegisterFunction("substring", new SQLFunctionTemplate(NHibernateUtil.String, "mid(?1, ?2, ?3)"));
          //RegisterFunction("locate",new JetLocateFunction());//new SQLFunctionTemplate(NHibernateUtil.Int32, "(instr(?3+1,?2,?1)-1)"));
           RegisterFunction("cast", new JetCastFunction());
           //LINQ not calling extract dialect function 
           //RegisterFunction("extract", new StandardSQLFunction("jetextract", NHibernateUtil.Int32));
           RegisterFunction("date", new SQLFunctionTemplate(NHibernateUtil.Date, "dateadd('d', 0, datediff('d', 0, ?1))"));
           RegisterFunction("coalesce", new JetCoalesceFunction());
           RegisterFunction("current_timestamp", new SQLFunctionTemplate(NHibernateUtil.DateTime, "Now"));
           RegisterFunction("sqrt", new SQLFunctionTemplate(NHibernateUtil.Double, "Sqr(?1)"));
           RegisterFunction("mod", new SQLFunctionTemplate(NHibernateUtil.Int32, "(?1 Mod ?2)"));
           RegisterFunction("nullif", new JetNullIfFunction());
           RegisterFunction("trim", new JetAnsiTrimEmulationFunction());



           RegisterColumnType(DbType.Int64, "INT");      
           RegisterColumnType(DbType.Decimal, "FLOAT");
           RegisterColumnType(DbType.Decimal, 19, "FLOAT");
          
       }

       
       /// <summary> The SQL literal value to which this database maps boolean values. </summary>
       /// <param name="value">The boolean value </param>
       /// <returns> The appropriate SQL literal. </returns>
       public override string ToBooleanValueString(bool value)
       {
           return value ? "-1" : "0";
       }



       /// <summary>
       /// Does this Dialect have some kind of <c>LIMIT</c> syntax?
       /// </summary>
       /// <value>True, we'll use the SELECT TOP nn syntax.</value>
       public override bool SupportsLimit
       {
           get { return true; }
       }


       /// <summary>
       /// Can parameters be used for a statement containing a LIMIT?
       /// </summary>
       public override bool SupportsVariableLimit
       {
           get { return true; }
       }

       /// <summary>
       /// Add a <c>LIMIT (TOP)</c> clause to the given SQL <c>SELECT</c>
       /// </summary>
       /// <param name="querySqlString">A Query in the form of a SqlString.</param>
       /// <param name="limit">Maximum number of rows to be returned by the query</param>
       /// <param name="offset">Offset of the first row to process in the result set</param>
       /// <returns>A new SqlString that contains the <c>LIMIT</c> clause.</returns>
       public override SqlString GetLimitString(SqlString querySqlString, SqlString offset, SqlString limit)
       {
           /*
			 * "SELECT TOP limit rest-of-sql-statement"
			 */

           SqlStringBuilder topFragment = new SqlStringBuilder();
           topFragment.Add(" top ");
           topFragment.Add(limit);

           return querySqlString.Insert(GetAfterSelectInsertPoint(querySqlString), topFragment.ToSqlString());
       }

       /// <summary>
       /// Does the <c>LIMIT</c> clause take a "maximum" row number
       /// instead of a total number of returned rows?
       /// </summary>
       /// <returns>false, unless overridden</returns>
       public override bool UseMaxForLimit
       {
           get { return true; }
       }

       private static int GetAfterSelectInsertPoint(SqlString sql)
       {
           if (sql.StartsWithCaseInsensitive("select distinct"))
           {
               return 15;
           }
           else if (sql.StartsWithCaseInsensitive("select"))
           {
               return 6;
           }
           throw new NotSupportedException("The query should start with 'SELECT' or 'SELECT DISTINCT'");
       }


       public override IDataBaseSchema GetDataBaseSchema(DbConnection connection)
       {
           var jetConnection = (JetDbConnection) connection;
           return new JetDataBaseSchema(jetConnection.Connection);
       }


    }
}
