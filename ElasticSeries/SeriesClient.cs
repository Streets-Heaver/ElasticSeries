using Nest;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public class SeriesClient
    {
        private readonly ElasticClient _elasticClient;
        public SeriesClient(ConnectionSettings settings)
        {
            _elasticClient = new ElasticClient(settings);
        }

        public void Record(string metricName, double value)
        {
            Record(metricName, value, DateTime.Now, null);
        }

        public void Record(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            Record(metricName, value, DateTime.Now, additionalProperties);
        }

        public void Record(string metricName, double value, DateTime time)
        {
            Record(metricName, value, time, null);
        }

        public void Record(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            dynamic metricData;
            metricData = new ExpandoObject();

            metricData.MetricName = metricName;
            metricData.Value = value;
            metricData.Time = time;

            if (additionalProperties != null && additionalProperties.Any())
            {
                var expando = metricData as IDictionary<string, object>;
                foreach (var additionalProperty in additionalProperties)
                {
                    expando.Add(additionalProperty.Key, additionalProperty.Value);
                }
            }

            _elasticClient.Index((object)metricData, idx => idx);

        }

        public async Task RecordASync(string metricName, double value)
        {
            await RecordAsync(metricName, value, DateTime.Now, null);
        }

        public async Task RecordAsync(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            await RecordAsync(metricName, value, DateTime.Now, additionalProperties);
        }

        public async Task RecordAsync(string metricName, double value, DateTime time)
        {
            await RecordAsync(metricName, value, time, null);
        }

        public async Task RecordAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {

            dynamic metricData;
            metricData = new ExpandoObject();

            metricData.MetricName = metricName;
            metricData.Value = value;
            metricData.Time = time;

            if (additionalProperties != null && additionalProperties.Any())
            {
                var expando = metricData as IDictionary<string, object>;
                foreach (var additionalProperty in additionalProperties)
                {
                    expando.Add(additionalProperty.Key, additionalProperty.Value);
                }
            }

            await _elasticClient.IndexAsync((object)metricData, idx => idx);

        }
    }
}
