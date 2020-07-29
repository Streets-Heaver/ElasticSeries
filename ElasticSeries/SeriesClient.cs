using Nest;
using System;

namespace ElasticSeries
{
    public class SeriesClient
    {
        private readonly ElasticClient _elasticClient;
        private readonly ConnectionSettings _settings;
        public SeriesClient(ConnectionSettings settings)
        {
            _settings = settings;
            _elasticClient = new ElasticClient(settings);
        }

        public void Record(string metricName, double value)
        {
            Record(metricName, value, DateTime.Now);
        }

        public void Record(string metricName, double value, DateTime time)
        {

            _elasticClient.Index(new TSData()
            {
                MetricName = metricName,
                Value = value,
                Time = time

            }, idx => idx);

        }
    }
}
