using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;
using NHibernate.Driver;

using NHibernate.JetDriver.SqlFixes;

namespace NHibernate.JetDriver
{
    public class JetDriverEx : JetDriver
    {
        /// <summary>Use Access with named parameter (p0, p1 ..) and not "?" </summary>
        /// <value>true to use named parameter</value>
        public override bool UseNamedPrefixInSql
        {
            get
            {
                return true;
            }
        }

        /// <summary>List of Fixes</summary>
        private SqlStringFix[] _sqlFixes ={new SqlStringFixExtract(),
                                            new SqlStringFixCaseWhen(),
                                            new SqlStringFixLocateFunction(),
                                            new SqlStringFixAggregateDistinct(),
                                            new SqlStringFixCastFunction(),
                                            new SqlStringFixOrderByAlias(),
                                            new SqlStringFixUpperLowerFunction()};

        /// <summary>Use GenerateCommand to fix Jet issues or temporary 
        ///          bugs of NHibernate that affect JetDriver
        /// </summary>
        /// <param name="type"></param>
        /// <param name="sqlString"></param>
        /// <param name="parameterTypes"></param>
        /// <returns></returns>
        public override IDbCommand GenerateCommand(CommandType type, SqlString sqlString, SqlType[] parameterTypes)
        {
            var parametersOriginal = new List<Parameter>();

            var sql = sqlString.ToString();

            foreach (var part in sqlString)
            {
                if (part is Parameter)
                {
                    parametersOriginal.Add((Parameter)part);
                }
            }

            Regex regexReplaceParam = new Regex(
              "\\?",
            RegexOptions.IgnoreCase
            | RegexOptions.Singleline
            | RegexOptions.CultureInvariant
            );

            int i = 0;
            foreach (var param in parametersOriginal)
            {
                if (param.ParameterPosition == null)
                {
                    param.ParameterPosition = i;
                }
                sql = regexReplaceParam.Replace(sql, String.Format("@p{0}", param.ParameterPosition), 1);
                i++;
            }


            SqlString final = sqlString;
            var sqlFixed = sql;

            if (_sqlFixes.Length > 0)
            {

                foreach (var sqlStringFix in _sqlFixes)
                {
                    sqlFixed = sqlStringFix.FixSql(sqlFixed);
                }

            }

            final = RestoreParameters(parametersOriginal, sqlFixed);

            if (final.IndexOfCaseInsensitive(" union ") < 0)
            {

                var fromParts = ExtractFromParts(final);

                foreach (var ansiJoinWithEndMarker in fromParts)
                {
                    var ansiFrom = ansiJoinWithEndMarker.Replace(JetJoinFragment.ENDJOIN, "");

                    var sb = new SqlStringBuilder();
                    string accessFrom;

                    sb.Add("SELECT *" + ansiFrom);
                    var sqlJetFrom = FinalizeJoins(sb.ToSqlString());
                    sqlJetFrom = sqlJetFrom.Replace("SELECT *", "");
                    accessFrom = sqlJetFrom.ToString();

                    var start = final.IndexOfCaseInsensitive(ansiJoinWithEndMarker);
                    sb = new SqlStringBuilder();
                    if (start > 0)
                    {
                        sb.Add(final.Substring(0, start));
                        sb.Add(accessFrom);
                        sb.Add(final.Substring(start + ansiJoinWithEndMarker.Length));
                        final = sb.ToSqlString();
                    }
                    else
                    {
                        throw new InvalidOperationException("invalid from sql. Verify if from part has parameters. Jet does not support from clause with parameters. " + ansiFrom);
                    }

                }

            }//union
            else
            {
                final = final.Replace(JetJoinFragment.ENDJOIN, "");
            }

            return base.GenerateCommandOleDBDriver(type, final, parameterTypes);

        }


        private List<string> ExtractFromParts(SqlString sqlString)
        {
            var parts = new List<string>();
            var sql = sqlString.ToString();
            var joinTags = ParseJoinNodes(sql);



            var i = 0;
            while (i < joinTags.Count)
            {
                var fromStart = -1;
                var fromEnd = -1;

                while ((i < joinTags.Count) && (joinTags[i].Name == FromClause))
                {
                    fromStart = joinTags[i].Position;
                    i++;
                }

                while ((i < joinTags.Count) && (joinTags[i].Name == JetJoinFragment.ENDJOIN))
                {
                    fromEnd = joinTags[i].Position;
                    i++;
                }

                if ((fromStart >= 0) && (fromEnd > fromStart))
                {
                    parts.Add(sql.Substring(fromStart, fromEnd + JetJoinFragment.ENDJOIN.Length - fromStart));
                }

            }

            return parts;

        }

        private List<JoinNode> ParseJoinNodes(string sql)
        {


            var joinTags = JoinTags(sql, FromClause);
            joinTags.AddRange(JoinTags(sql, JetJoinFragment.ENDJOIN));

            var q = from joinTag in joinTags
                    orderby joinTag.Position
                    select joinTag;

            return q.ToList();

        }

        private List<JoinNode> JoinTags(string sql, string joinClause)
        {
            var joinTags = new List<JoinNode>();
            var startIndex = 0;
            var fromIndex = sql.IndexOf(joinClause, startIndex, StringComparison.InvariantCultureIgnoreCase);
            while (fromIndex > 0)
            {
                var joinNode = new JoinNode();
                joinNode.Position = fromIndex;
                joinNode.Name = joinClause;
                joinTags.Add(joinNode);
                startIndex = fromIndex + 1;
                fromIndex = sql.IndexOf(joinClause, startIndex, StringComparison.InvariantCultureIgnoreCase);
            }
            return joinTags;
        }

        public const string FromClause = " from ";


        private class JoinNode
        {
            public string Name { get; set; }
            public int Position { get; set; }
        }

        private SqlString RestoreParameters(List<Parameter> parametersOriginal, string sqlfixed)
        {

            Regex regexReplaceParamNamed = new Regex(
              "@p\\d+",
            RegexOptions.IgnoreCase
            | RegexOptions.Singleline
            | RegexOptions.CultureInvariant
            );

            var matches = regexReplaceParamNamed.Matches(sqlfixed);
            var parts = regexReplaceParamNamed.Split(sqlfixed);

            var sqlBuilder = new SqlStringBuilder();

            for (int i = 0; i < parts.Length; i++)
            {
                if (i > 0)
                {
                    var paramPos = matches[i - 1].Value.Replace("@p", "");
                    var param = parametersOriginal.Where(p => p.ParameterPosition == Convert.ToInt32(paramPos)).First();
                    sqlBuilder.AddObject(param);
                }

                sqlBuilder.Add(parts[i]);
            }

            return sqlBuilder.ToSqlString();

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
            if (parametersOriginal.Count == parametersTransformed.Count)
            {
                return;
            }

            //fix missing parameters spliting around '?'  
            var sqlText = transformedSQL.ToString();
            Regex regex = new Regex("@x\\d+",
                                    RegexOptions.IgnoreCase
                                    | RegexOptions.CultureInvariant
                                    );

            var parametersParts = regex.Split(sqlText);

            //parametersParts = sqlText.Split('?');

            if ((parametersParts.Length - 1) != parametersOriginal.Count)
            {
                //can´t restore 
                var msg = "FinalizeJoins JetDriver removed SQL parameteres and can not be restored";
                throw new QueryException(msg);
            }

            var sqlBuilder = new SqlStringBuilder();

            for (int i = 0; i < parametersParts.Length; i++)
            {
                if (i > 0)
                {
                    sqlBuilder.AddObject(parametersOriginal[i - 1]);
                }

                sqlBuilder.Add(parametersParts[i]);
            }

            transformedSQL = sqlBuilder.ToSqlString();

        }




    }
}
