using Nest;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries.Classes.Gauges
{
    public class AggregateGauge : IGauge
    {
        private readonly string _metricName;
        private readonly Dictionary<string, object> _additionalProperties;
        private readonly int _batchSize;
        private readonly ElasticClient _elasticClient;
        private List<double> _batchData = new List<double>();


        public AggregateGauge(ElasticClient elasticClient, string metricName, int batchSize, Dictionary<string, object> additionalProperties)
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
        /// <returns>Ids of any commited data as a result of this action</returns>
        public string Record(double value)
        {
            _batchData.Add(value);

            if (_batchData.Count == _batchSize)
                return CommitAggregate();
            else
                return string.Empty;
        }


        /// <summary>
        /// Records a data point that will be sent to Elasticsearch. 
        /// </summary>
        /// <param name="value">Value of metric</param>
        /// <returns>Ids of any commited data as a result of this action</returns>
        public async Task<string> RecordAsync(double value)
        {

            _batchData.Add(value);

            if (_batchData.Count == _batchSize)
                return await CommitAggregateAsync();
            else
                return string.Empty;

        }



        /// <summary>
        /// Commits aggregate of current batched data
        /// </summary>
        /// <returns></returns>
        public string CommitAggregate()
        {
            if (_batchData.Any())
            {
                var document = _elasticClient.IndexDocument(BuildAggregateDocument());
                _batchData = new List<double>();

                return document.Id;
            }
            else
                return string.Empty;
        }

        /// <summary>
        /// Commits aggregate of current batched data
        /// </summary>
        /// <returns></returns>
        public async Task<string> CommitAggregateAsync()
        {
            if (_batchData.Any())
            {
                var document = await _elasticClient.IndexDocumentAsync(BuildAggregateDocument());

                _batchData = new List<double>();

                return document.Id;
            }
            else
                return string.Empty; 

        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public void Dispose()
        {
            CommitAggregate();
        }

        /// <summary>
        /// Commits any batched data to Elasticsearch
        /// </summary>
        /// <returns></returns>
        public async ValueTask DisposeAsync()
        {
            await CommitAggregateAsync();
        }

        /// <summary>
        /// Returns count of current batch data awaiting commit
        /// </summary>
        /// <returns></returns>
        public int GetCurrentBatchCount()
        {
            return _batchData.Count;
        }

        private dynamic BuildAggregateDocument()
        {
            dynamic aggregateData;
            aggregateData = new ExpandoObject();

            aggregateData.MetricName = _metricName;
            aggregateData.Time = DateTime.Now;

            aggregateData.Average = _batchData.Average();
            aggregateData.Max = _batchData.Max();
            aggregateData.Min = _batchData.Min();
            aggregateData.Last = _batchData.Last();
            aggregateData.Count = _batchData.Count();

            if (_additionalProperties != null && _additionalProperties.Any())
            {
                var expando = aggregateData as IDictionary<string, object>;
                foreach (var additionalProperty in _additionalProperties)
                {
                    expando.Add(additionalProperty.Key, additionalProperty.Value);
                }
            }

            return aggregateData;
        }
    }
}
