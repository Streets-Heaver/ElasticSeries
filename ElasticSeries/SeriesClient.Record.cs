using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public partial class SeriesClient
    {
        private List<dynamic> _batchData = new List<dynamic>();
        private int _batchSize;

        public IEnumerable<string> Record(string metricName, double value, bool forcePush = false)
        {
            return Record(metricName, value, DateTime.Now, null, forcePush);
        }

        public IEnumerable<string> Record(string metricName, double value, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            return Record(metricName, value, DateTime.Now, additionalProperties, forcePush);
        }

        public IEnumerable<string> Record(string metricName, double value, DateTime time, bool forcePush = false)
        {
            return Record(metricName, value, time, null, forcePush);
        }

        public IEnumerable<string> Record(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize || forcePush)
                return FlushBatch();
            else
                return Enumerable.Empty<string>();
        }

        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, DateTime.Now, null, forcePush);
        }

        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, DateTime.Now, additionalProperties, forcePush);
        }

        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, DateTime time, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, time, null, forcePush);
        }

        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize || forcePush)
                return await FlushBatchAsync();
            else
                return Enumerable.Empty<string>();

        }

        public IEnumerable<string> FlushBatch()
        {
            var documents = _elasticClient.IndexMany(_batchData);
            _batchData = new List<dynamic>();
            return documents.Items.Select(x => x.Id);
        }

        public async Task<IEnumerable<string>> FlushBatchAsync()
        {
            var documents = await _elasticClient.IndexManyAsync(_batchData);
            _batchData = new List<dynamic>();
            return documents.Items.Select(x => x.Id);

        }

        public void Dispose()
        {
            FlushBatch();
        }

        public async ValueTask DisposeAsync()
        {
            await FlushBatchAsync();
        }

        public int GetCurrentBatchCount()
        {
            return _batchData.Count;
        }

    }
}
