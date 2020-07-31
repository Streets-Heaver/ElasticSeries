using Nest;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public partial class SeriesClient
    {
        private List<dynamic> _batchData = new List<dynamic>();
        private int _batchSize;

        public void RecordBatchItem(string metricName, double value)
        {
            RecordBatchItem(metricName, value, DateTime.Now, null);
        }

        public void RecordBatchItem(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            RecordBatchItem(metricName, value, DateTime.Now, additionalProperties);
        }

        public void RecordBatchItem(string metricName, double value, DateTime time)
        {
            RecordBatchItem(metricName, value, time, null);
        }

        public void RecordBatchItem(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize)
                FlushBatch();
        }

        public async Task RecordBatchItemAsync(string metricName, double value)
        {
            await RecordBatchItemAsync(metricName, value, DateTime.Now, null);
        }

        public async Task RecordBatchItemAsync(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            await RecordBatchItemAsync(metricName, value, DateTime.Now, additionalProperties);
        }

        public async Task RecordBatchItemAsync(string metricName, double value, DateTime time)
        {
            await RecordBatchItemAsync(metricName, value, time, null);
        }

        public async Task RecordBatchItemAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize)
                await FlushBatchAsync();
        }

        public void FlushBatch()
        {
            _elasticClient.IndexMany(_batchData);
        }

        public async Task FlushBatchAsync()
        {
            await _elasticClient.IndexManyAsync(_batchData);
        }

        public void Dispose()
        {
            FlushBatch();
        }

        public async ValueTask DisposeAsync()
        {
            await FlushBatchAsync();
        }

    }
}
