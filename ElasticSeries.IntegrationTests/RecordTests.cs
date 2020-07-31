using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace ElasticSeries.IntegrationTests
{
    [TestClass]
    public class RecordTests
    {
        [TestMethod]
        public void AddsToElasticSearch_BeTrue()
        {
            var settings = new Nest.ConnectionSettings(new System.Uri("http://172.16.69.85:9200"));
            settings.DefaultIndex("testindex");
            var client = new SeriesClient(settings);

            var additional = new Dictionary<string, object>();
            additional.Add("TestProperty1", "Test");
            additional.Add("TestProperty2", 1);
            additional.Add("TestProperty3", true);

            client.Record("test", 400, additional);


        }
    }
}
