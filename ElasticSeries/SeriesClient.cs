using ElasticSeries.Classes;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSeries
{
    public partial class SeriesClient : IDisposable, IAsyncDisposable 
    {
        private readonly ElasticClient _elasticClient;

        private Dictionary<string, IGauge> _activeGauges;
       
        public SeriesClient(string configFile) :
            this(new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(configFile)).ElasticSearchUrl)))
        {
        }

        public SeriesClient(ConnectionSettings settings)
        {
            _activeGauges = new Dictionary<string, IGauge>();
            _elasticClient = new ElasticClient(settings);
        }


        public MetricGauge CreateMetricGauge(string metricName)
        {
            return CreateMetricGauge(metricName, 100, null);
        }

        public MetricGauge CreateMetricGauge(string metricName, int batchSize)
        {
            return CreateMetricGauge(metricName, batchSize, null);
        }

        public MetricGauge CreateMetricGauge(string metricName, int batchSize, Dictionary<string, object> additionalProperties)
        {
            _activeGauges.Add(metricName, new MetricGauge(_elasticClient, metricName, batchSize, additionalProperties));

            return _activeGauges[metricName] as MetricGauge;
        }

        public void Dispose()
        {
            foreach (var gauge in _activeGauges)
                gauge.Value.Dispose();

            _activeGauges.Clear();
        }

    
        public async ValueTask DisposeAsync()
        {
            foreach (var gauge in _activeGauges)
                await gauge.Value.DisposeAsync();

            _activeGauges.Clear();

        }
    }
}
