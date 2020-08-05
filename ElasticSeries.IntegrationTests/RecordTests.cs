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
    public class RecordTests
    {

        [TestMethod]
        public void RecordsAreSavedWhenForcePushed_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var id = client.Record("test", 400, forcePush: true);

            id.Should().NotBeNull();

        }

        [TestMethod]
        public async Task RecordsAreSavedWhenForcePushedAsync_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var id = await client.RecordAsync("test", 400, forcePush: true);

            id.Should().NotBeNull();

        }


        [TestMethod]
        public void BatchRecordsAreSaved_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            client.Record("test", 400);
            client.Record("test", 300);
            client.Record("test", 420);
            client.Record("test", 410);
            client.Record("test", 200);
            client.Record("test", 324);
            client.Record("test", 542);
            client.Record("test", 401);
            client.Record("test", 434);
            client.Record("test", 290);

            var ids = client.FlushBatch();
            ids.Count().Should().Be(10);

        }


        [TestMethod]
        public void BatchIsSavedWhenBatchIsFull_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings, 10);

            client.Record("test", 400);
            client.Record("test", 300);
            client.Record("test", 420);
            client.Record("test", 410);
            client.Record("test", 200);
            client.Record("test", 324);
            client.Record("test", 542);
            client.Record("test", 401);
            client.Record("test", 434);
            var save = client.Record("test", 290);

            save.Should().NotBeEmpty();
        }

        [TestMethod]
        public void BatchIsNotSavedWhenBatchIsNotFull_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings, 20);

            client.Record("test", 400);
            client.Record("test", 300);
            client.Record("test", 420);
            client.Record("test", 410);
            client.Record("test", 200);
            client.Record("test", 324);
            client.Record("test", 542);
            client.Record("test", 401);
            client.Record("test", 434);
            var save = client.Record("test", 290);

            save.Should().BeEmpty();

        }

        [TestMethod]
        public void DisposeFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings, 20);

            client.Record("test", 400);
            client.Record("test", 300);
            client.Record("test", 420);
            client.Record("test", 410);
            client.Record("test", 200);
            client.Record("test", 324);
            client.Record("test", 542);
            client.Record("test", 401);
            client.Record("test", 434);
            client.Record("test", 290);

            client.Dispose();

            client.GetCurrentBatchCount().Should().Be(0);

        }

        [TestMethod]
        public async Task DisposeAsyncFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings, 20);

            await client.RecordAsync("test", 400);
            await client.RecordAsync("test", 300);

            await client.DisposeAsync();

            client.GetCurrentBatchCount().Should().Be(0);


        }



        [TestMethod]
        public async Task BatchRecordsAreSavedAsync_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            await client.RecordAsync("test", 400);
            await client.RecordAsync("test", 300);
            await client.RecordAsync("test", 420);
            await client.RecordAsync("test", 410);
            await client.RecordAsync("test", 200);
            await client.RecordAsync("test", 324);
            await client.RecordAsync("test", 542);
            await client.RecordAsync("test", 401);
            await client.RecordAsync("test", 434);
            await client.RecordAsync("test", 290);

            var ids = await client.FlushBatchAsync();
            ids.Count().Should().Be(10);


        }

        [TestMethod]
        public async Task BatchIsSavedWhenBatchIsFullAsync_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings, 10);

            await client.RecordAsync("test", 400);
            await client.RecordAsync("test", 300);
            await client.RecordAsync("test", 420);
            await client.RecordAsync("test", 410);
            await client.RecordAsync("test", 200);
            await client.RecordAsync("test", 324);
            await client.RecordAsync("test", 542);
            await client.RecordAsync("test", 401);
            await client.RecordAsync("test", 434);

            var save = await client.RecordAsync("test", 290);

            save.Should().NotBeEmpty();

        }

        [TestMethod]
        public async Task BatchIsNotSavedWhenBatchIsNotFullAsync_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings, 20);

            await client.RecordAsync("test", 400);
            await client.RecordAsync("test", 300);
            await client.RecordAsync("test", 420);
            await client.RecordAsync("test", 410);
            await client.RecordAsync("test", 200);
            await client.RecordAsync("test", 324);
            await client.RecordAsync("test", 542);
            await client.RecordAsync("test", 401);
            await client.RecordAsync("test", 434);
            await client.RecordAsync("test", 290);

            var save = await client.RecordAsync("test", 290);

            save.Should().BeEmpty();


        }


        [TestMethod]
        public async Task CallingDisposeWhenBatchIsEmptyDoesntErrorAsync_NotThrow()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            Func<Task> act = async () =>
            {
                await using (var client = new SeriesClient(settings))
                {

                }

            };
            await act.Should().NotThrowAsync<ArgumentException>();

        }

        [TestMethod]
        public void CallingDisposeWhenBatchIsEmptyDoesntError_NotThrow()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            Action act = () =>
            {
                using (var client = new SeriesClient(settings))
                {

                }
            };
            act.Should().NotThrow<ArgumentException>();

        }
    }
}
