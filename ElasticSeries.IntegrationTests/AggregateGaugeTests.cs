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
using System.Threading;
using System.Threading.Tasks;

namespace ElasticSeries.IntegrationTests
{
    [TestClass]
    public class AggregateGaugeTests
    {

        [TestMethod]
        public void AggregateSaved_NotBeNullOrEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test");

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

            var id = gauge.CommitAggregate();
            id.Should().NotBeNullOrEmpty();

        }


        [TestMethod]
        public void AggregateSavedSavedWhenBatchIsFull_NotBeNullOrEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 10);

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

            save.Should().NotBeNullOrEmpty();
        }

        [TestMethod]
        public void AggregateNotSavedWhenBatchIsNotFull_BeEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 20);

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
        public void DisposeCommitsAggregate_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 20);

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
        public async Task DisposeAsyncCommitsAggregate_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 20);

            await gauge.RecordAsync(400);
            await gauge.RecordAsync(300);

            await gauge.DisposeAsync();

            gauge.GetCurrentBatchCount().Should().Be(0);


        }



        [TestMethod]
        public async Task AggregateSavedAsync_NotBeNullOrEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test");

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

            var id = await gauge.CommitAggregateAsync();
            id.Should().NotBeNullOrEmpty();


        }

        [TestMethod]
        public async Task AggregateSavedSavedWhenBatchIsFullAsync_NotBeEmpty()
        {

            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 10);

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

            save.Should().NotBeNullOrEmpty();

        }

        [TestMethod]
        public async Task AggregateNotSavedWhenBatchIsNotFullAsync_BeNullOrEmpty()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            await using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test", 20);
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

            save.Should().BeNullOrEmpty();
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
                    client.CreateAggregateGauge("test", 20);

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
                    client.CreateAggregateGauge("test", 20);
                }
            };
            act.Should().NotThrow<ArgumentException>();

        }

        [TestMethod]
        public void DataCommitedWhenTimerTicks_Be0()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test");
            gauge.StartTimer(TimeSpan.FromSeconds(2));

            gauge.Record(400);

            Thread.Sleep(TimeSpan.FromSeconds(3));

            gauge.GetCurrentBatchCount().Should().Be(0);
        }

        [TestMethod]
        public void CanOnlyStartOneTimerPerGauge_ThrowsInvalidOperationException()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test");
            gauge.StartTimer(TimeSpan.FromSeconds(2));

            Action act = () =>
            {
                gauge.StartTimer(TimeSpan.FromSeconds(2));

            };

            act.Should().Throw<InvalidOperationException>();
        }

        [TestMethod]
        public void CanOnlyStopWhenTimerRunning_ThrowsInvalidOperationException()
        {
            var settings = new ConnectionSettings(new Uri(JsonConvert.DeserializeObject<Config>(File.ReadAllText(Path.Combine(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.Parent.Parent.FullName, "es.json"))).ElasticSearchUrl));
            settings.DefaultIndex("elasticseriestest");

            using var client = new SeriesClient(settings);

            var gauge = client.CreateAggregateGauge("test");

            Action act = () =>
            {
                gauge.StopTimer();

            };

            act.Should().Throw<InvalidOperationException>();
        }
    }
}
