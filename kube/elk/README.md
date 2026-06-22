# ELK stack (kube/elk)

Elasticsearch, Logstash, and Kibana, deployed into the `fleet` namespace of the
k3d cluster. This is the searchable log/event store and its query UI. It runs
alongside `obs-svc-agg` (the in-memory Protobuf aggregator one level up in
`kube/`); the two are independent and deploy separately.

## Components

| Component | Image | In-cluster address | Edge |
|-----------|-------|--------------------|------|
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.15.3` | `elasticsearch:9200` | none (internal) |
| Kibana | `docker.elastic.co/kibana/kibana:8.15.3` | `kibana:5601` | `kibana.local` via traefik |
| Logstash | `docker.elastic.co/logstash/logstash:8.15.3` | `logstash:5044` (Beats), `:9600` (API) | none (internal) |

Elasticsearch is a single-node StatefulSet with a dynamic `local-path` PVC (5Gi).
Kibana and Logstash are stateless Deployments. Kibana is the only component with
an edge route, following the one-edge model: cloudflared -> traefik (`web`) ->
`kibana` Service.

## Trust model

`xpack.security` is off. This matches the rest of the fleet's in-cluster posture
(kafka and schema-registry are PLAINTEXT): the trust boundary is the edge
(Cloudflare Access in front of cloudflared), not the data plane. TLS and auth on
Elasticsearch are a later, separate change, deferred the same way they are for
kafka. No credentials are stored here and none are omitted.

## What ingests data today

Logstash runs the stock Beats-in / Elasticsearch-out pipeline
(`kube/elk/logstash.yaml`). Anything shipped to `logstash:5044` lands in the
`fleet-logs-*` daily indices and is queryable in Kibana.

## Not done yet: the Kafka -> Logstash bridge

Logstash does **not** consume the fleet's Kafka bus. The bus carries
Confluent-framed Protobuf (schema-registry magic byte + schema id, then the
Protobuf payload) on topics such as `delight.events`. Stock Logstash cannot
decode that: it needs the Protobuf codec plugin, compiled message descriptors,
and a schema-registry-aware deframer. That is a build, not stock configuration,
so it is intentionally out of scope here and tracked as a follow-up. Until it
exists, the Kafka -> store path is owned by `obs-svc-agg`, which already speaks
the Protobuf contracts.

## Deploy

Images are already present in the local docker daemon. Load them into k3d (host
-> cluster copy, no network) and apply:

```sh
for img in \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.3 \
  docker.elastic.co/kibana/kibana:8.15.3 \
  docker.elastic.co/logstash/logstash:8.15.3; do
  k3d image import "$img" -c fleet
done

kubectl apply -k kube/elk/
```

Validate manifests without applying:

```sh
kubectl apply --dry-run=client -k kube/elk/
```

## Reach it

Kibana is routed at `kibana.local` through traefik. Resolve that host to the
cluster edge (the same way the other `*.local` fleet hosts are reached), or
port-forward directly:

```sh
kubectl -n fleet port-forward svc/kibana 5601:5601
# then open http://localhost:5601
```
