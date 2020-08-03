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

        public IEnumerable<string> RecordBatchItem(string metricName, double value)
        {
            return RecordBatchItem(metricName, value, DateTime.Now, null);
        }

        public IEnumerable<string> RecordBatchItem(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            return RecordBatchItem(metricName, value, DateTime.Now, additionalProperties);
        }

        public IEnumerable<string> RecordBatchItem(string metricName, double value, DateTime time)
        {
            return RecordBatchItem(metricName, value, time, null);
        }

        public IEnumerable<string> RecordBatchItem(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize)
                return FlushBatch();
            else
                return Enumerable.Empty<string>();
        }

        public async Task<IEnumerable<string>> RecordBatchItemAsync(string metricName, double value)
        {
            return await RecordBatchItemAsync(metricName, value, DateTime.Now, null);
        }

        public async Task<IEnumerable<string>> RecordBatchItemAsync(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            return await RecordBatchItemAsync(metricName, value, DateTime.Now, additionalProperties);
        }

        public async Task<IEnumerable<string>> RecordBatchItemAsync(string metricName, double value, DateTime time)
        {
            return await RecordBatchItemAsync(metricName, value, time, null);
        }

        public async Task<IEnumerable<string>> RecordBatchItemAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize)
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
