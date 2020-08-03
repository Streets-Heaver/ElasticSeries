using ElasticSeries.Classes;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Threading.Tasks;

namespace ElasticSeries.IntegrationTests
{
    [TestClass]
    public class RecordTests
    {
        [TestMethod]
        public void RecordsAreSaved_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings);

            var id = client.Record("test", 400);

            id.Should().NotBeNull();

        }

        [TestMethod]
        public async Task RecordsAreSavedaAsync_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings);

            var id = await client.RecordAsync("test", 400);

            id.Should().NotBeNull();

        }
    }
}
