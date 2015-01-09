using System.Collections;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;

using log4net;

using NHibernate.Driver;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernate.JetDriver
{
	/// <summary>
	/// Implementation of IDriver for Jet database engine.
	/// Because of the weird JOIN clause syntax, this class has to translate the queries generated by NHibernate
	/// into the Jet syntax. This cannot be done anywhere else without having to heavily modify the logic of query creation.
	/// The translations of queries are cached.
	/// </summary>
	public class JetDriver : OleDbDriver
	{
		private static readonly ILog logger = LogManager.GetLogger(typeof(JetDriver));

        private const string FromClause = " from ";
        private const string WhereClause = " where ";
	    private const string OrderByClause = " order by ";

		private readonly IDictionary _queryCache = new Hashtable();

        protected IDbCommand GenerateCommandOleDBDriver(CommandType type, SqlString sqlString, SqlType[] parameterTypes)
        {
            return base.GenerateCommand(type, sqlString, parameterTypes);
        }

		public override IDbCommand GenerateCommand(CommandType type, SqlString sqlString, SqlType[] parameterTypes)
		{
		    SqlString final = IsSelectStatement(sqlString) ? FinalizeJoins(sqlString) : sqlString;
		    return base.GenerateCommand(type, final, parameterTypes);
		}

	    /// <summary></summary>
		public override IDbConnection CreateConnection()
		{
			return new JetDbConnection();
		}

		/// <summary>
		/// We have to have a special db command type to support conversion of data types, because Access is weird.
		/// </summary>
		public override IDbCommand CreateCommand()
		{
			return new JetDbCommand();
		}

		/// <summary>
		/// MS Access expects @paramName
		/// </summary>
		public override bool UseNamedPrefixInParameter
		{
			get { return true; }
		}

		public override string NamedPrefix
		{
			get { return "@"; }
		}

		#region Query transformations

		/// <summary>
		///Jet engine has the following from clause syntax:
		///<code>
		///		tableexpression[, tableexpression]*
		///</code>
		///where tableexpression is:
		///<code>
		///		tablename [(INNER |LEFT | RIGHT) JOIN [(] tableexpression [)] ON ...]
		///</code>
		///where the parenthesises are necessary if the "inner" tableexpression is not just a single tablename.
		///Additionally INNER JOIN cannot be nested in LEFT | RIGHT JOIN.
		///To translate the simple non-parenthesized joins to the jet syntax, the following transformation must be done:
		///<code>
		///		A join B on ... join C on ... join D on ..., E join F on ... join G on ..., H join I on ..., J
		///has to be translated as:
		///		(select * from ((A join B on ...) join C on ...) join D on ...) as crazyAlias1, (select * from (E join F on ...) join G on ...) as crazyAlias2, (select * from H join I on ...) as crazyAlias3, J
		///</code>
		/// </summary>
		/// <param name="sqlString">the sqlstring to transform</param>
		/// <returns>sqlstring with parenthesized joins.</returns>
		protected SqlString FinalizeJoins(SqlString sqlString)
		{
			if (_queryCache.Contains(sqlString))
			{
				return (SqlString) _queryCache[sqlString];
			}

            var beginOfFrom = sqlString.IndexOfCaseInsensitive(FromClause);
			var endOfFrom = sqlString.IndexOfCaseInsensitive(WhereClause);
			var beginOfOrderBy = sqlString.IndexOfCaseInsensitive(OrderByClause);
            
			if (beginOfFrom < 0)
			{
				return sqlString;
			}

            if (beginOfOrderBy < 0)
            {
                if (endOfFrom < 0)
                {
                    endOfFrom = sqlString.Length;
                }
            }
            else
            {
                endOfFrom = beginOfOrderBy;
            }

			var fromClause = sqlString.Substring(beginOfFrom, endOfFrom - beginOfFrom).ToString();
            var wherePart = sqlString.Substring(endOfFrom);
            var fromClauseInWhere = wherePart.IndexOfCaseInsensitive(FromClause);
			var transformedFrom = TransformFromClause(fromClause);
            var processWhereJoins = string.Empty;

            if (fromClauseInWhere > -1) //has where clause, inspect other joins
            {
                var whereClause = wherePart.Substring(0, fromClauseInWhere);
                var criteria = wherePart.Substring(fromClauseInWhere).ToString();

                processWhereJoins = whereClause + TransformFromClause(criteria);
            }

			//put it all together again
			var final = new SqlStringBuilder(sqlString.Count + 1);
			final.Add(sqlString.Substring(0, beginOfFrom));
			final.Add(transformedFrom);

            if (string.IsNullOrEmpty(processWhereJoins))
            {
			final.Add(sqlString.Substring(endOfFrom));
            }
            else
            {
                final.Add(processWhereJoins);
            }

			SqlString ret = final.ToSqlString();

            RestoreMissingParameters(sqlString, ref ret);

			_queryCache[sqlString] = ret;

			return ret;
		}

        private void RestoreMissingParameters(SqlString originalSQL, ref SqlString transformedSQL)
        {
            if (originalSQL.Equals(transformedSQL))
             {
                     return;
             }
             
            var parametersOriginal = new ArrayList();
            var parametersTransformed = new ArrayList();


            foreach (var part in originalSQL)
            {
                if (part is Parameter)
                {
                    parametersOriginal.Add(part);
                }
            }

            foreach (var part in transformedSQL)
            {
                if (part is Parameter)
                {
                    parametersTransformed.Add(part);
                }
            }

            //same number of parameters , return 
            if (parametersOriginal.Count==parametersTransformed.Count)
            {
                return; 
            }

            //fix missing parameters spliting around '?'  
            var sqlText  = transformedSQL.ToString();
            var parametersParts = sqlText.Split('?');

            if ((parametersParts.Length - 1) != parametersOriginal.Count)
            {
                //can�t restore 
                var msg =  "FinalizeJoins JetDriver removed SQL parameteres and can not be restored"; 
                logger.Error(msg);
				throw new QueryException(msg);
            }

            var sqlBuilder = new SqlStringBuilder();

            for (int i = 0; i < parametersParts.Length; i++)
            {
                if (i>0)
	            {
            	  sqlBuilder.AddObject(parametersOriginal[i-1]);  	 
	            }

                sqlBuilder.Add(parametersParts[i]);
            }

            transformedSQL = sqlBuilder.ToSqlString();
                   
        }

		private string TransformFromClause(string fromClause)
		{
			string transformed;

			int fromLength = FromClause.Length;
			fromClause = fromClause.Substring(fromLength, fromClause.Length - fromLength);
			string[] blocks = fromClause.Split(',');
			if (blocks.Length > 1)
			{
				for (int i = 0; i < blocks.Length; i++)
				{
					string tr = TransformJoinBlock(blocks[i]);
					if (tr.IndexOf(" join ") > -1)
					{
						blocks[i] = "(select * from " + tr + ") as jetJoinAlias" + i;
					}
					else
					{
						blocks[i] = tr;
					}
				}

				transformed = string.Join(",", blocks);
			}
			else
			{
				transformed = TransformJoinBlock(blocks[0]);
			}

            return FromClause + transformed;
		}

        /// <summary>
        /// Transforms the join block
        /// </summary>
        /// <param name="block">A string representing one join block.</param>
        /// <returns></returns>
		private static string TransformJoinBlock(string block)
		{
			int parenthesisCount = 0;

			Regex re = new Regex(" join");
			string[] blockParts = re.Split(block);

			if (blockParts.Length > 1)
			{
				for (int i = 1; i < blockParts.Length; i++)
				{
					string part = blockParts[i];
					int parenthesisIndex = -1;

					if (part.EndsWith(" inner"))
					{
						parenthesisIndex = part.Length - 6;
					}
					else if (part.EndsWith(" left outer"))
					{
						parenthesisIndex = part.Length - 11;
					}
					else if (part.EndsWith(" right outer"))
					{
						parenthesisIndex = part.Length - 12;
					}

					if (parenthesisIndex == -1)
					{
						if (i < blockParts.Length - 1)
						{
							logger.Error("Invalid join syntax. Could not parenthesize the join block properly.");
							throw new QueryException("Invalid join syntax. Could not parenthesize the join block properly.");
						}

						//everything went ok. I'm processing the last block part and I've got no parenthesis to add.
						var b = new StringBuilder(" ");
						for (int j = 0; j < parenthesisCount; j++)
						{
							b.Append("(");
						}
						b.Append(string.Join(" join", blockParts));

						return b.ToString();
					}
					else
					{
						parenthesisCount++;
						blockParts[i] = part.Insert(parenthesisIndex, ")");
					}
				}

				//the last block part contained the join. This should not happen.
				logger.Error("Invalid join syntax. Could not parenthesize the join block properly.");
				throw new QueryException("Invalid join syntax. Could not parenthesize the join block properly.");
			}
			else
			{
				return blockParts[0];
			}
		}

		private static bool IsSelectStatement(SqlString sqlString)
		{
			return sqlString.StartsWithCaseInsensitive("select");
		}

		#endregion
	}
}
