using System;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using NHibernate.Dialect.Schema;
using Iesi.Collections.Generic;
using System.Collections.Generic;

namespace NHibernate.JetDriver.Schema
{
	public class JetDataBaseSchema: AbstractDataBaseSchema
	{
	    public JetDataBaseSchema(DbConnection connection) : base(connection)
		{
            _Connection = (OleDbConnection)connection;
		}

        private OleDbConnection _Connection;

		public override ITableMetadata GetTableMetadata(DataRow rs, bool extras)
		{

            return new JetTableMetadata(rs, this, extras);
		}

        public override DataTable GetForeignKeys(string catalog, string schema, string table)
        {
           //How To Retrieve Schema Information by Using GetOleDbSchemaTable and Visual Basic .NET
           //http://support.microsoft.com/kb/309488
           var oledbConnection = (OleDbConnection)Connection;
           object[] restrictions = new object[] { null, null, table, null };
            
            // Open the schema information for the foreign keys.
            var schemaTable = oledbConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys, restrictions);

            return schemaTable;

        }

        public override DataTable GetIndexColumns(string catalog, string schemaPattern, string tableName, string indexName)
        {

            //How To Retrieve Schema Information by Using GetOleDbSchemaTable and Visual Basic .NET
            //http://support.microsoft.com/kb/309488
            var oledbConnection = (OleDbConnection)Connection;
            object[] restrictions = new object[] { null, null, tableName, null };

            // Open the schema information for indexes.
            var schemaTable = oledbConnection.GetOleDbSchemaTable(OleDbSchemaGuid.Indexes, restrictions);

            return schemaTable;
        }

        public override ISet<string> GetReservedWords()
        {
            var reserverWords =  base.GetReservedWords();
            reserverWords.Add("action");
            return reserverWords;
        }


	}

    /// <summary>
    ///    Convert Dataschema native number types to Jetdriver typenames 
    /// </summary>
    public class JetDataTypes {
       
        private static Dictionary<string, string> _JetDriverTypes;
       
        static JetDataTypes()
        {
            _JetDriverTypes = new Dictionary<string, string>();
            _JetDriverTypes.Add("2", "SMALLINT");
            _JetDriverTypes.Add("3", "INT");
            _JetDriverTypes.Add("4", "REAL");
            _JetDriverTypes.Add("5", "FLOAT");
            _JetDriverTypes.Add("6", "MONEY");
            _JetDriverTypes.Add("7", "DATETIME");
            _JetDriverTypes.Add("11", "BIT");
            _JetDriverTypes.Add("17", "BYTE");
            _JetDriverTypes.Add("72", "GUID");
            _JetDriverTypes.Add("204", "IMAGE");//BigBinary
            _JetDriverTypes.Add("205", "IMAGE");//longBinary
            _JetDriverTypes.Add("203", "MEMO");//LongText
            _JetDriverTypes.Add("202", "TEXT");
            _JetDriverTypes.Add("131", "DECIMAL");
            _JetDriverTypes.Add("130", "TEXT");     
        }

        public static string GetJetDriverTypeName(string nativeDatatype)
        {
            return _JetDriverTypes[nativeDatatype];
        }

    }

	public class JetTableMetadata: AbstractTableMetadata
	{

		public JetTableMetadata(DataRow rs, IDataBaseSchema meta, bool extras) : base(rs, meta, extras)
		{
		}

	    protected override void ParseTableInfo(DataRow rs)
		{
			Catalog = Convert.ToString(rs["TABLE_CATALOG"]);
			Schema = Convert.ToString(rs["TABLE_SCHEMA"]);
			if (string.IsNullOrEmpty(Catalog)) Catalog = null;
			if (string.IsNullOrEmpty(Schema)) Schema = null;
			Name = Convert.ToString(rs["TABLE_NAME"]);
		}

		protected override string GetConstraintName(DataRow rs)
		{
            return Convert.ToString(rs["FK_NAME"]);
		}

		protected override string GetColumnName(DataRow rs)
		{
			return Convert.ToString(rs["COLUMN_NAME"]);
		}

		protected override string GetIndexName(DataRow rs)
		{
			return Convert.ToString(rs["INDEX_NAME"]);
		}

		protected override IColumnMetadata GetColumnMetadata(DataRow rs)
		{
            return new JetColumnMetadata(rs);
		}

		protected override IForeignKeyMetadata GetForeignKeyMetadata(DataRow rs)
		{
			return new JetForeignKeyMetadata(rs);
		}

		protected override IIndexMetadata GetIndexMetadata(DataRow rs)
		{
			return new JetIndexMetadata(rs);
		}
	}

	public class JetColumnMetadata : AbstractColumnMetaData
	{
		public JetColumnMetadata(DataRow rs) : base(rs)
		{
			Name = Convert.ToString(rs["COLUMN_NAME"]);
			object aValue;

			aValue = rs["CHARACTER_MAXIMUM_LENGTH"];
			if (aValue != DBNull.Value)
				ColumnSize = Convert.ToInt32(aValue);

			aValue = rs["NUMERIC_PRECISION"];
			if (aValue != DBNull.Value)
				NumericalPrecision = Convert.ToInt32(aValue);

			Nullable = Convert.ToString(rs["IS_NULLABLE"]);
			TypeName =JetDataTypes.GetJetDriverTypeName(Convert.ToString(rs["DATA_TYPE"]));			
		}
	}

	public class JetIndexMetadata: AbstractIndexMetadata
	{
		public JetIndexMetadata(DataRow rs) : base(rs)
		{
			Name = Convert.ToString(rs["INDEX_NAME"]);
		}
	}

	public class JetForeignKeyMetadata : AbstractForeignKeyMetadata
	{
		public JetForeignKeyMetadata(DataRow rs)
			: base(rs)
		{
            Name = Convert.ToString(rs["FK_NAME"]);
		}
	}
}
