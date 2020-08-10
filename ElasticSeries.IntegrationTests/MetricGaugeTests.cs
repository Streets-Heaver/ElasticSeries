using ElasticSeries.Classes;
using FluentAssertions;
using FluentAssertions.Execution;
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
    public class MetricGaugeTests
    {

        [TestMethod]
        public void RecordsAreSavedWhenForcePushed_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var id = client.CreateMetricGauge("test").Record(400, forcePush: true);

            id.Should().NotBeNull();

        }

        [TestMethod]
        public void ForcePushOnlyCommitsPassedData_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test");
            gauge.Record(400);

            var id = gauge.Record(410, forcePush: true);

            using (var scope = new AssertionScope())
            {
                id.Should().NotBeNull();
                gauge.GetCurrentBatchCount().Should().Be(1);
            }

        }

        [TestMethod]
        public async Task RecordsAreSavedWhenForcePushedAsync_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var id = await client.CreateMetricGauge("test").RecordAsync(400, forcePush: true);

            id.Should().NotBeNull();

        }

        [TestMethod]
        public async Task ForcePushOnlyCommitsPassedDataAsync_NotBeNull()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test");
            await gauge.RecordAsync(400);

            var id = await gauge.RecordAsync(410, forcePush: true);

            using (var scope = new AssertionScope())
            {
                id.Should().NotBeNull();
                gauge.GetCurrentBatchCount().Should().Be(1);
            }

        }


        [TestMethod]
        public void BatchRecordsAreSaved_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test");

            gauge.Record(400);
            gauge.Record(300);
            gauge.Record(420);
            gauge.Record(410);
            gauge.Record(200);
            gauge.Record(324);
            gauge.Record(542);
            gauge.Record(401);
            gauge.Record(434);
            gauge.Record(290);

            var ids = gauge.FlushBatch();
            ids.Count().Should().Be(10);

        }


        [TestMethod]
        public void BatchIsSavedWhenBatchIsFull_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 10);

            gauge.Record(400);
            gauge.Record(300);
            gauge.Record(420);
            gauge.Record(410);
            gauge.Record(200);
            gauge.Record(324);
            gauge.Record(542);
            gauge.Record(401);
            gauge.Record(434);
            var save = gauge.Record(290);

            save.Should().NotBeEmpty();
        }

        [TestMethod]
        public void BatchIsNotSavedWhenBatchIsNotFull_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 20);

            gauge.Record(400);
            gauge.Record(300);
            gauge.Record(420);
            gauge.Record(410);
            gauge.Record(200);
            gauge.Record(324);
            gauge.Record(542);
            gauge.Record(401);
            gauge.Record(434);
            var save = gauge.Record(290);

            save.Should().BeEmpty();

        }

        [TestMethod]
        public void DisposeFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 20);

            gauge.Record(400);
            gauge.Record(300);
            gauge.Record(420);
            gauge.Record(410);
            gauge.Record(200);
            gauge.Record(324);
            gauge.Record(542);
            gauge.Record(401);
            gauge.Record(434);
            gauge.Record(290);

            gauge.Dispose();

            gauge.GetCurrentBatchCount().Should().Be(0);

        }

        [TestMethod]
        public async Task DisposeAsyncFlushesBatch_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 20);

            await gauge.RecordAsync(400);
            await gauge.RecordAsync(300);

            await gauge.DisposeAsync();

            gauge.GetCurrentBatchCount().Should().Be(0);


        }



        [TestMethod]
        public async Task BatchRecordsAreSavedAsync_Be10()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test");

            await gauge.RecordAsync(400);
            await gauge.RecordAsync(300);
            await gauge.RecordAsync(420);
            await gauge.RecordAsync(410);
            await gauge.RecordAsync(200);
            await gauge.RecordAsync(324);
            await gauge.RecordAsync(542);
            await gauge.RecordAsync(401);
            await gauge.RecordAsync(434);
            await gauge.RecordAsync(290);

            var ids = await gauge.FlushBatchAsync();
            ids.Count().Should().Be(10);


        }

        [TestMethod]
        public async Task BatchIsSavedWhenBatchIsFullAsync_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 10);

            await gauge.RecordAsync(400);
            await gauge.RecordAsync(300);
            await gauge.RecordAsync(420);
            await gauge.RecordAsync(410);
            await gauge.RecordAsync(200);
            await gauge.RecordAsync(324);
            await gauge.RecordAsync(542);
            await gauge.RecordAsync(401);
            await gauge.RecordAsync(434);

            var save = await gauge.RecordAsync(290);

            save.Should().NotBeEmpty();

        }

        [TestMethod]
        public async Task BatchIsNotSavedWhenBatchIsNotFullAsync_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateMetricGauge("test", 20);
            await gauge.RecordAsync(400);
            await gauge.RecordAsync(300);
            await gauge.RecordAsync(420);
            await gauge.RecordAsync(410);
            await gauge.RecordAsync(200);
            await gauge.RecordAsync(324);
            await gauge.RecordAsync(542);
            await gauge.RecordAsync(401);
            await gauge.RecordAsync(434);
            await gauge.RecordAsync(290);

            var save = await gauge.RecordAsync(290);

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
                   client.CreateMetricGauge("test", 20);

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
                    client.CreateMetricGauge("test", 20);
                }
            };
            act.Should().NotThrow<ArgumentException>();

        }
    }
}
