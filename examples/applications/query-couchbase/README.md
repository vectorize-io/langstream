<!-- TODO Change for couchbase -->

# Querying a Couchbase Cluster

This sample application shows how to perform queries against a Coushbase Cluster.

## Prerequisites

Create an Cluster and bucket on Couchbase.


## Preparing the Couchbase Cluster

Create your Couchbase Cluster following the official documentation.
https://docs.couchbase.com/cloud/index.html

Create a new bucket with any name you desire.

You also have to set your OpenAI API keys in the secrets.yaml file. 

## Configure access to the Vector Database

Export some ENV variables in order to configure access to the database:

```
export COUCHBASE_BUCKET_NAME=...
export COUCHBASE_USERNAME=...
export COUCHBASE_PASSWORD=...
export COUCHBASE_CONNECTION_STRING=...
```

Use the same bucket name you created.
For the username and password you will need to create a new 'Database Access' user. You can find this in the Settings/Database Access tab.
(Ensure you give the user the 'Data Read/Write' role in the bucket you created.)
You can find the connection string from the Couchbase web interface in the Connect tab.

```
couchbases://cb.shnnjztaidekg6i.cloud.couchbase.com
```

The above is an example of a connection string.


## Deploy the LangStream application

```
./bin/langstream apps deploy test -app examples/applications/query-couchbase -i examples/instances/kafka-kubernetes.yaml -s examples/secrets/secrets.yaml
```
Using the docker image:

```
./bin/langstream docker run test -app examples/applications/query-couchbase -s examples/secrets/secrets.yaml
```

## Send a message using the gateway to index a document

```
bin/langstream gateway produce test write-topic -v "{\"document\":\"Hello\"}" -p sessionId=$(uuidgen) -k "{\"sessionId\":\"$(uuidgen)\"}"
```
You can view the uploaded document in the _default scope and _default collection of the bucket you selected.
<!-- 
## Start a chat using the gateway to query the index

```
 bin/langstream gateway chat test -pg produce-input -cg consume-output -p sessionId=$(uuidgen)
 ```

 Send a JSON string with at matching question:

```
{"question": "Hello"}
```

## Start a Producer to index a document

Let's start a produce that sends messages to the vectors-topic:

```
kubectl -n kafka run kafka-producer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic vectors-topic
```

Insert a JSON with "id" and "document" and a genre:

```json
{"id": "myid", "document": "Hello", "genre": "comedy"}
```

The Write pipeline will compute the embeddings on the "document" field and then write a Vector into Pinecone.

## Start a Producer to Trigger a query

```
kubectl -n kafka run kafka-producer-question -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-producer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic input-topic
```

Insert a JSON with a "question":

```json
{"question": "Hello"}
```


## Start a Consumer

Start a Kafka Consumer on a terminal

```
kubectl -n kafka run kafka-consumer -ti --image=quay.io/strimzi/kafka:0.35.1-kafka-3.4.0 --rm=true --restart=Never -- bin/kafka-console-consumer.sh --bootstrap-server my-cluster-kafka-bootstrap:9092 --topic output-topic --from-beginning
``` -->

