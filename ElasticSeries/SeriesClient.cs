using Nest;
using System;

namespace ElasticSeries
{
    public class SeriesClient
    {
        private readonly ElasticClient _elasticClient;
        public SeriesClient(ConnectionSettings settings)
        {
            _elasticClient = new ElasticClient(settings);
        }
    }
}
