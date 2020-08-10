using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace ElasticSeries.Classes
{
    public class GaugeBase
    {
        protected dynamic BuildDocument(string metricName, double value, DateTime time, Dictionary<string, object> additionalProperties)
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
