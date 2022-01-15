# Covid-Twitter-Real-time-Sentiment-Analysis in Python using Kubernetes, PubSub, and BigQuery

This Project uses the [Google Kubernetes Engine](https://cloud.google.com/kubernetes-engine/) (GKE) and [BigQuery](https://cloud.google.com/bigquery/what-is-bigquery) to build a 'pipeline' to stream Twitter data, via [Google Cloud PubSub](https://cloud.google.com/pubsub/docs) using Python.

Kubernetes Engine is Google's managed version of [Kubernetes](http://github.com/GoogleCloudPlatform/kubernetes),
an open source container orchestrator originally developed by Google, and now managed by a community of contributors.
 For more information [docs](https://kubernetes.io/docs/home/).

[Bigquery](https://cloud.google.com/bigquery/what-is-bigquery)  lets you run fast, SQL-like queries against multi-terabyte datasets in seconds, using the processing power of Google's infrastructure.

[PubSub](https://cloud.google.com/pubsub/overview) provides many-to-many, asynchronous messaging that decouples senders and receivers. It allows for secure and highly available communication between independently written applications and delivers low-latency, durable messaging.

The app uses uses PubSub to buffer the data coming in from Twitter and to decouple ingestion from processing.
One of the Kubernetes app *pods* reads the data from Twitter and publishes it to a PubSub topic.  Other pods subscribe to the PubSub topic, grab data in small batches, and stream it into BigQuery.  The figure below suggests this flow.

<img src="http://amy-jo.storage.googleapis.com/images/k8s_pubsub_tw_bq.png" width="680">

This app can be thought of as a 'workflow' type of app-- it doesn't have a web
front end (though Kubernetes is great for those types of apps as well).
Instead, it is designed to continously run a scalable data ingestion pipeline.
Note that PubSub provides [guaranteed at-least-once message
delivery](https://cloud.google.com/pubsub/overview#benefits).  This means that
we might sometimes see a duplicated item, but as each tweet has a UID, So, that's
not an issue for this project.
