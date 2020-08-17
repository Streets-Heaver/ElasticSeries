using ElasticSeries.Gauges;
using Nest;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries.Gauges
{
    public class MetricGauge : IGauge
    {
        private readonly string _metricName;
        private readonly Dictionary<string, object> _additionalProperties;
        private readonly int _batchSize;
        private readonly ElasticClient _elasticClient;
        private List<dynamic> _batchData = new List<dynamic>();


        public MetricGauge(ElasticClient elasticClient, string metricName, int batchSize, Dictionary<string, object> additionalProperties)
        {

            if (string.IsNullOrEmpty(metricName))
                throw new InvalidOperationException("You must specify a metric name");

            _batchSize = batchSize;
            _metricName = metricName;
            _additionalProperties = additionalProperties;
            _elasticClient = elasticClient;
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="value">Value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(double value, bool forcePush = false)
        {
            return Record(value, DateTime.Now, forcePush);
        }

        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public IEnumerable<string> Record(double value, DateTime time, bool forcePush = false)
        {
            if (forcePush)
            {
                var documents = _elasticClient.IndexMany(new List<dynamic>() { BuildDocument(value, time) });
                return documents.Items.Select(x => x.Id);
            }
            else
            {
                _batchData.Add(BuildDocument(value, time));

                if (_batchData.Count == _batchSize)
                    return FlushBatch();
                else
                    return Enumerable.Empty<string>();
            }
        }



        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="value">Value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(double value, bool forcePush = false)
        {
            return await RecordAsync(value, DateTime.Now, forcePush);
        }



        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="value">Value of metric</param>
        /// <param name="time">Date/Time value of metric</param>
        /// <param name="forcePush">When true will instantly commit the data without waiting for batch size to be met</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<IEnumerable<string>> RecordAsync(double value, DateTime time, bool forcePush = false)
        {
            if (forcePush)
            {
                var documents = await _elasticClient.IndexManyAsync(new List<dynamic>() { BuildDocument(value, time) });
                return documents.Items.Select(x => x.Id);
            }
            else
            {
                _batchData.Add(BuildDocument(value, time));

                if (_batchData.Count == _batchSize)
                    return await FlushBatchAsync();
                else
                    return Enumerable.Empty<string>();
            }
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

        private dynamic BuildDocument(double value, DateTime time)
        {
            dynamic metricData;
            metricData = new ExpandoObject();

            metricData.MetricName = _metricName;
            metricData.Value = value;
            metricData.Time = time;

            if (_additionalProperties != null && _additionalProperties.Any())
            {
                var expando = metricData as IDictionary<string, object>;
                foreach (var additionalProperty in _additionalProperties)
                {
                    expando.Add(additionalProperty.Key, additionalProperty.Value);
                }
            }

            return metricData;
        }
    }
}
