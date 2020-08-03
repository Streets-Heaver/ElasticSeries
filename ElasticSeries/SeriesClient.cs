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
        public SeriesClient(ConnectionSettings settings) : this(settings, 100)
        {

        }

        public SeriesClient(string configFile) : this(configFile, 100)
        {

        }

        public SeriesClient(string configFile, int batchSize) :
            this(new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(configFile)).ElasticSearchUrl)), batchSize)
        {
          
        }

        public SeriesClient(ConnectionSettings settings, int batchSize)
        {
            _elasticClient = new ElasticClient(settings);
            _batchSize = batchSize;
        }

        private dynamic BuildDocument(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
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

            return metricData;
        }
    }
}
