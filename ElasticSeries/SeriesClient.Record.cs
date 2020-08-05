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

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(string metricName, double value, bool forcePush = false)
        {
            return Record(metricName, value, DateTime.Now, null, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="additionalProperties">Allows storing of any additional data</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(string metricName, double value, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            return Record(metricName, value, DateTime.Now, additionalProperties, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(string metricName, double value, DateTime time, bool forcePush = false)
        {
            return Record(metricName, value, time, null, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. Additional Properites can be set so any additional data can be stored with metric data. Force Push optional parameter can be used to instantly commit the data to Elasticsearch without waiting for the batch size to be met.
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="additionalProperties">Allows storing of any additional data</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize || forcePush)
                return FlushBatch();
            else
                return Enumerable.Empty<string>();
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, DateTime.Now, null, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="additionalProperties">Allows storing of any additional data</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, DateTime.Now, additionalProperties, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, DateTime time, bool forcePush = false)
        {
            return await RecordAsync(metricName, value, time, null, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. Additional Properites can be set so any additional data can be stored with metric data. Force Push optional parameter can be used to instantly commit the data to Elasticsearch without waiting for the batch size to be met.
        /// </summary>
        /// <param name="metricName">Name of metric</param>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="additionalProperties">Allows storing of any additional data</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties, bool forcePush = false)
        {
            _batchData.Add(BuildDocument(metricName, value, time, additionalProperties));

            if (_batchData.Count == _batchSize || forcePush)
                return await FlushBatchAsync();
            else
                return Enumerable.Empty<string>();

        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> FlushBatch()
        {
            if (_batchData.Any())
            {
                var documents = _elasticClient.IndexMany(_batchData);
                _batchData = new List<dynamic>();
                return documents.Items.Select(x => x.Id);
            }
            else
                return Enumerable.Empty<string>();
        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> FlushBatchAsync()
        {
            if (_batchData.Any())
            {
                var documents = await _elasticClient.IndexManyAsync(_batchData);
                _batchData = new List<dynamic>();
                return documents.Items.Select(x => x.Id);
            }
            else
                return Enumerable.Empty<string>();

        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public void Dispose()
        {
            FlushBatch();
        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            await FlushBatchAsync();
        }

        /// <summary>
        /// Returns count of current batch data awaiting commit
        /// </summary>
        /// <returns></returns>
        public int GetCurrentBatchCount()
        {
            return _batchData.Count;
        }

    }
}
