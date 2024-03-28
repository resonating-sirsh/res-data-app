# Google Analytics Data Integration

## Overview

This app queries Google Analytics APIs to retrieve data regarding sessions, 
events, conversions, and general traffic for Google Analytics accounts 
corresponding to Resonance brands.

The app makes use of the [Google Analytics API python client](https://github.com/googleapis/google-api-python-client/)
to retrieve data, parse it into several distinct datasets, and send that data to
Snowflake. Data exists in the RAW or RAW_DEV database within the 
GOOGLE_ANALYTICS schema. Data is requested via a batch query method which allows
for the submission of multiple graphql-like queries at once. Queries can have a 
maximum of seven dimensions. 

## Dimensions and Metrics

Dimensions are attributes of data and metrics are aggregations of attributes of 
data. In SQL terms, dimensions would be a series of fields being grouped by
and metrics would be the fields using an aggregate function such as max or min.
Available dimensions and metrics are viewable within 
[Google's interactive dimensions and metrics explorer](https://ga-dev-tools.web.app/dimensions-metrics-explorer/). 

## Sampling in Default Reports and Ad-Hoc Queries
Google Analytics calculates certain combinations of dimensions and metrics on 
the fly. To return the data in a reasonable time, Google Analytics may only 
process a sample of the data.

The sampling level to use for a request is changed by setting the samplingLevel 
parameter. Within a given batch query request. In this application that sampling
level is always set to "Large," which provides greater data accuracy but at the
cost of query execution speed.

If a report contains sampled data the Analytics Reporting API v4 (API used to 
retrieve Universal Analytics data) returns the samplesReadCounts and the 
samplingSpaceSizes fields. If the results are not sampled these fields will not 
be defined.These 2 values can be used to calculate the percentage of sessions 
that were used for the query. For example, if sampleSize is 201,000 and 
sampleSpace is 220,000 then the report is based on 
(201,000 / 220,000) * 100 = 91.36% of sessions for the date range in the query.

Default reports (the reports available on the side of the GA website or reports 
using the same dimensions as those reports) are not subject to sampling. Any 
report not matching those defaults has the following rules applied:

* For Universal Analytics, sampling tends to occur if the date range in the query
request contains too many sessions for the property being queried. For analytics 
standard accounts that limit is 500k and for analytics 360 accounts that limit
is 100M at the view level. Queries may include events, custom variables, and 
custom dimensions and metrics. All other queries have a threshold of 1M

* In GA4 that limit is at the property level (since views don't exist) and the 
threshold is roughly 10 million events in a given date range. That GA4 limit is 
not explicitly stated in documentation. 

Information derived and adapted from 
* [the Core API v4 development guide](https://developers.google.com/analytics/devguides/reporting/core/v4/basics#sampling).
* [Community answers](https://support.google.com/analytics/answer/2637192#thresholds&zippy=%2Cin-this-article)

## Sampling in Other Reports
Sampling works differently for these reports than for default reports or 
ad-hoc queries.

### Multi-Channel Funnels reports
Like default reports, no sampling is applied unless the default settings for the
report are modified in some way. For example by changing the look-back window, 
by changing which conversions are included, or by adding a segment or secondary 
dimension. If reports are modified a maximum sample of 1M conversions will be 
returned.

### Flow-visualization reports
Flow-visualization reports (Users Flow, Behavior Flow, Events Flow, Goal Flow) 
are generated from a maximum of 100K sessions for the selected date range.

The flow-visualization reports, including entrance, exit, and conversion rates, 
may differ from the results in the default Behavior and Conversions reports, 
which are based on a different sample set.

## Request Limits
The following quotas apply to all Reporting APIs, including the Core Reporting 
API v3, Analytics Reporting API v4, Real Time API v3, and Multi-channel Funnel 
API v3:

* 10,000 requests per view (profile) per day (cannot be increased)
* 10 concurrent requests per view (profile) (cannot be increased)

## Misc. Links Related to the GA4 API

* https://developers.google.com/analytics/devguides/reporting/core/v4/samples#dimensions_and_metrics
* https://developers.google.com/analytics/devguides/reporting/core/v4/basics
* https://developers.google.com/analytics/devguides/reporting/core/v4/advanced#ltv
* https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet#ReportRequest
* https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py
