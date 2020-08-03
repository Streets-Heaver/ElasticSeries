using ElasticSeries.Classes;
using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Nest;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ElasticSeries.IntegrationTests
{
    [TestClass]
    public class RecordBatchItemTests
    {
        [TestMethod]
        public void BatchRecordsAreSaved_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings);

            client.RecordBatchItem("test", 400);
            client.RecordBatchItem("test", 300);
            client.RecordBatchItem("test", 420);
            client.RecordBatchItem("test", 410);
            client.RecordBatchItem("test", 200);
            client.RecordBatchItem("test", 324);
            client.RecordBatchItem("test", 542);
            client.RecordBatchItem("test", 401);
            client.RecordBatchItem("test", 434);
            client.RecordBatchItem("test", 290);

            var ids = client.FlushBatch();
            ids.Count().Should().Be(10);

        }

       
        [TestMethod]
        public void BatchIsSavedWhenBatchIsFull_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 10);

            client.RecordBatchItem("test", 400);
            client.RecordBatchItem("test", 300);
            client.RecordBatchItem("test", 420);
            client.RecordBatchItem("test", 410);
            client.RecordBatchItem("test", 200);
            client.RecordBatchItem("test", 324);
            client.RecordBatchItem("test", 542);
            client.RecordBatchItem("test", 401);
            client.RecordBatchItem("test", 434);
            var save = client.RecordBatchItem("test", 290);

            save.Should().NotBeEmpty();
        }

        [TestMethod]
        public void BatchIsNotSavedWhenBatchIsNotFull_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 20);

            client.RecordBatchItem("test", 400);
            client.RecordBatchItem("test", 300);
            client.RecordBatchItem("test", 420);
            client.RecordBatchItem("test", 410);
            client.RecordBatchItem("test", 200);
            client.RecordBatchItem("test", 324);
            client.RecordBatchItem("test", 542);
            client.RecordBatchItem("test", 401);
            client.RecordBatchItem("test", 434);
            var save = client.RecordBatchItem("test", 290);

            save.Should().BeEmpty();

        }

        [TestMethod]
        public void DisposeFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 20);

            client.RecordBatchItem("test", 400);
            client.RecordBatchItem("test", 300);
            client.RecordBatchItem("test", 420);
            client.RecordBatchItem("test", 410);
            client.RecordBatchItem("test", 200);
            client.RecordBatchItem("test", 324);
            client.RecordBatchItem("test", 542);
            client.RecordBatchItem("test", 401);
            client.RecordBatchItem("test", 434);
            client.RecordBatchItem("test", 290);

            client.Dispose();

            client.GetCurrentBatchCount().Should().Be(0);

        }

        [TestMethod]
        public async Task DisposeAsyncFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 20);

            client.RecordBatchItem("test", 400);
            client.RecordBatchItem("test", 300);
            client.RecordBatchItem("test", 420);
            client.RecordBatchItem("test", 410);
            client.RecordBatchItem("test", 200);
            client.RecordBatchItem("test", 324);
            client.RecordBatchItem("test", 542);
            client.RecordBatchItem("test", 401);
            client.RecordBatchItem("test", 434);
            client.RecordBatchItem("test", 290);

            await client.DisposeAsync();

            client.GetCurrentBatchCount().Should().Be(0);

        }



        [TestMethod]
        public async Task BatchRecordsAreSavedAsync_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings);

            await client.RecordBatchItemAsync("test", 400);
            await client.RecordBatchItemAsync("test", 300);
            await client.RecordBatchItemAsync("test", 420);
            await client.RecordBatchItemAsync("test", 410);
            await client.RecordBatchItemAsync("test", 200);
            await client.RecordBatchItemAsync("test", 324);
            await client.RecordBatchItemAsync("test", 542);
            await client.RecordBatchItemAsync("test", 401);
            await client.RecordBatchItemAsync("test", 434);
            await client.RecordBatchItemAsync("test", 290);

            var ids = await client.FlushBatchAsync();
            ids.Count().Should().Be(10);

        }

        [TestMethod]
        public async Task BatchIsSavedWhenBatchIsFullAsync_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 10);

            await client.RecordBatchItemAsync("test", 400);
            await client.RecordBatchItemAsync("test", 300);
            await client.RecordBatchItemAsync("test", 420);
            await client.RecordBatchItemAsync("test", 410);
            await client.RecordBatchItemAsync("test", 200);
            await client.RecordBatchItemAsync("test", 324);
            await client.RecordBatchItemAsync("test", 542);
            await client.RecordBatchItemAsync("test", 401);
            await client.RecordBatchItemAsync("test", 434);

            var save = await client.RecordBatchItemAsync("test", 290);

            save.Should().NotBeEmpty();
        }

        [TestMethod]
        public async Task BatchIsNotSavedWhenBatchIsNotFullAsync_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            var client = new SeriesClient(settings, 20);

            await client.RecordBatchItemAsync("test", 400);
            await client.RecordBatchItemAsync("test", 300);
            await client.RecordBatchItemAsync("test", 420);
            await client.RecordBatchItemAsync("test", 410);
            await client.RecordBatchItemAsync("test", 200);
            await client.RecordBatchItemAsync("test", 324);
            await client.RecordBatchItemAsync("test", 542);
            await client.RecordBatchItemAsync("test", 401);
            await client.RecordBatchItemAsync("test", 434);
            await client.RecordBatchItemAsync("test", 290);

            var save = await client.RecordBatchItemAsync("test", 290);

            save.Should().BeEmpty();

        }
    }
}
