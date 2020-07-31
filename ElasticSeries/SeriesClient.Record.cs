using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public partial class SeriesClient
    {
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
            _elasticClient.Index((object)BuildDocument(metricName, value, time, additionalProperties), idx => idx);
        }

        public async Task RecordAsync(string metricName, double value)
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
            await _elasticClient.IndexAsync((object)BuildDocument(metricName, value, time, additionalProperties), idx => idx);
        }
    }
}
