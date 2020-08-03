using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public partial class SeriesClient
    {
        public string Record(string metricName, double value)
        {
            return Record(metricName, value, DateTime.Now, null);
        }

        public string Record(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            return Record(metricName, value, DateTime.Now, additionalProperties);
        }

        public string Record(string metricName, double value, DateTime time)
        {
            return Record(metricName, value, time, null);
        }

        public string Record(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            var response = _elasticClient.Index((object)BuildDocument(metricName, value, time, additionalProperties), idx => idx);
            return response.Id;
        }

        public async Task<string> RecordAsync(string metricName, double value)
        {
            return await RecordAsync(metricName, value, DateTime.Now, null);
        }

        public async Task<string> RecordAsync(string metricName, double value, Dictionary<string, object> additionalProperties)
        {
            return await RecordAsync(metricName, value, DateTime.Now, additionalProperties);
        }

        public async Task<string> RecordAsync(string metricName, double value, DateTime time)
        {
            return await RecordAsync(metricName, value, time, null);
        }

        public async Task<string> RecordAsync(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
        {
            var response = await _elasticClient.IndexAsync((object)BuildDocument(metricName, value, time, additionalProperties), idx => idx);
            return response.Id;
        }
    }
}
