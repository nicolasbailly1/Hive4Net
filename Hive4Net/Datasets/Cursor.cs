using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using Hive4Net.enums;
using Hive4Net.Requests;
using Hive4Net.Responses;
using Hive4Net.TCliService;

namespace Hive4Net.Datasets
{
    public class Cursor : IDisposable
    {
        private SessionHandle _session;
        private Client _client;
        private OperationHandle _operation;
        private TableSchema _lastSchema;
        private ProtocolVersion m_Version;

        public Cursor(SessionHandle session, Client client, ProtocolVersion version = ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7)
        {
            this._session = session;
            this._client = client;
            m_Version = version;
        }


        protected virtual async Task Dispose(bool disposing)
        {
            if (disposing)
            {
                await CloseOperationAsync();
                _lastSchema = null;
                _client = null;
                _session = null;
            }
        }

        public async Task ExecuteAsync(string statement)
        {
            await CloseOperationAsync();
            var execReq = new ExecuteStatementReq()
            {
                SessionHandle = _session,
                Statement = statement,
            };
            _lastSchema = null;
            var execResp = await _client.ExecuteStatementAsync(execReq);
            execResp.Status.CheckStatus();
            _operation = execResp.OperationHandle;
        }

        public async Task<List<ExpandoObject>> FetchManyAsync(int size)
        {
            var result = new List<ExpandoObject>();
            var names = await GetColumnNamesAsync();
            var rowSet = await FetchAsync(size);
            if (rowSet == null) return new List<ExpandoObject>();
            return GetRows(names, rowSet);
        }

        public async Task<ExpandoObject> FetchOne()
        {
            return (await FetchManyAsync(1)).FirstOrDefault();
        }

        #region GetRows

        private List<ExpandoObject> GetRows(List<string> names, RowSet rowSet)
        {
            List<ExpandoObject> result = new List<ExpandoObject>();
            if (m_Version <= ProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5)
            {
                result.AddRange(GetRowByRowBase(names, rowSet));
            }
            else if (!names.IsEmpty() && !rowSet.Columns.IsEmpty())
            {
                result.AddRange(GetRowByColumnBase(rowSet.Columns, names));
            }

            return result;
        }

        private IEnumerable<ExpandoObject> GetRowByRowBase(List<string> names, RowSet rowSet)
        {
            return rowSet.Rows.Select(j =>
            {
                var obj = new ExpandoObject();
                var dict = obj as IDictionary<string, object>;
                for (int i = 0; i < j.ColVals.Count; i++)
                {
                    dict.Add(names[i], GetRowValue(j.ColVals[i]));
                }
                return obj;
            });
        }

        private IEnumerable<ExpandoObject> GetRowByColumnBase(List<Column> columns, List<string> columnNames)
        {
            var list = columns.Select(GetRowValue).ToArray();
            int totalRows = list[0].Count;
            for (int i = 0; i < totalRows; i++)
            {
                var obj = new ExpandoObject();
                var dict = obj as IDictionary<string, object>;
                for (int j = 0; j < columnNames.Count; j++)
                {
                    dict.Add(columnNames[j], list[j][i]);
                }
                yield return obj;
            }
        }

        private IList GetRowValue(Column value)
        {
            if (value._isSet.stringVal)
                return value.StringVal.Values;
            else if (value._isSet.i32Val)
                return value.I32Val.Values;
            else if (value._isSet.boolVal)
                return value.BoolVal.Values;
            else if (value._isSet.doubleVal)
                return value.DoubleVal.Values;
            else if (value._isSet.byteVal)
                return value.ByteVal.Values;
            else if (value._isSet.i64Val)
                return value.I64Val.Values;
            else if (value._isSet.i16Val)
                return value.I16Val.Values;
            else
                return null;
        }

        private object GetRowValue(ColumnValue value)
        {
            if (value._isSet.stringVal)
                return value.StringVal.Value;
            else if (value._isSet.i32Val)
                return value.I32Val.Value;
            else if (value._isSet.boolVal)
                return value.BoolVal.Value;
            else if (value._isSet.doubleVal)
                return value.DoubleVal.Value;
            else if (value._isSet.byteVal)
                return value.ByteVal.Value;
            else if (value._isSet.i64Val)
                return value.I64Val.Value;
            else if (value._isSet.i16Val)
                return value.I16Val.Value;
            else
                return null;
        }
        #endregion

        public async Task<RowSet> FetchAsync(int count = int.MaxValue)
        {
            if (_operation == null || !_operation.HasResultSet) return null;
            var req = new FetchResultsReq()
            {
                MaxRows = count,
                Orientation = FetchOrientation.FETCH_NEXT,
                OperationHandle = _operation,
            };
            var resultsResp = await _client.FetchResultsAsync(req);
            resultsResp.Status.CheckStatus();
            return resultsResp.Results;
        }

        private async Task<List<string>> GetColumnNamesAsync()
        {
            var schema = await GetSchemaAsync();
            return schema?.Columns.Select(i => i.ColumnName).ToList();
        }

        public async Task<TableSchema> GetSchemaAsync()
        {
            if (_operation == null || !_operation.HasResultSet) return null;
            else if (_lastSchema == null)
            {
                var req = new GetResultSetMetadataReq(_operation);
                var resp = await _client.GetResultSetMetadataAsync(req);
                resp.Status.CheckStatus();
                _lastSchema = resp.Schema;
            }
            return _lastSchema;
        }

        private async Task CloseOperationAsync()
        {
            if (_operation != null)
            {
                CloseOperationReq closeReq = new CloseOperationReq();
                closeReq.OperationHandle = _operation;
                CloseOperationResp closeOperationResp = await _client.CloseOperationAsync(closeReq);
                closeOperationResp.Status.CheckStatus();
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
    }
}
