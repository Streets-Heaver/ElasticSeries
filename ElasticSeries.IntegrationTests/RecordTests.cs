using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ElasticSeries.IntegrationTests
{
    [TestClass]
    public class RecordTests
    {
        [TestMethod]
        public void AddsToElasticSearch_BeTrue()
        {
            var settings = new Nest.ConnectionSettings(new System.Uri("http://127.0.0.1:9200"));
            settings.DefaultIndex("testindex");
            var client = new SeriesClient(settings);
            client.Record("test", 400);


        }
    }
}
