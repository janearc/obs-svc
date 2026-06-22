# ELK + Grafana (kube/elk)

A fleet of services talks all day -- health beats, backup events, the ordinary
noise of things working and occasionally not. The trouble with talk is that it
vanishes the moment it's spoken unless something writes it down. This stack is
where the talk goes to be written down, and the two reading rooms you visit to
find it again.

Think of it as a small library. Elasticsearch is the stacks: everything shelved
so you can pull any one thing back out fast. Logstash is the returns desk, where
new material arrives and gets catalogued onto the right shelf. Kibana and Grafana
are two reading rooms over the same stacks -- Kibana is the Elastic-native desk
for digging through raw documents; Grafana is where those documents become
dashboards (and where the fleet's metrics dashboards will live too). It all runs
in the `fleet` namespace of the k3d cluster, next to `obs-svc-agg`, and the two
are independent -- you can redeploy either without disturbing the other.

## What's in the building

| Component | Image | In-cluster address | Reading room? |
|-----------|-------|--------------------|---------------|
| Elasticsearch | `docker.elastic.co/elasticsearch/elasticsearch:8.15.3` | `elasticsearch:9200` | the stacks (internal) |
| Logstash | `docker.elastic.co/logstash/logstash:8.15.3` | `logstash:5044` (Beats), `:9600` (API) | the returns desk (internal) |
| Kibana | `docker.elastic.co/kibana/kibana:8.15.3` | `kibana:5601` | yes -- `kibana.local` |
| Grafana | `grafana/grafana:11.3.0` | `grafana:3000` | yes -- `grafana.local` |

Elasticsearch is a single-node StatefulSet on a 5Gi dynamic `local-path` volume.
The other three are stateless Deployments. Only the two reading rooms face the
edge, and they do it the way everything here does: cloudflared -> traefik
(`web`) -> the service. No service gets its own door.

## Who's allowed in

The short answer: inside the cluster, everyone, and that's on purpose. Security
is off on Elasticsearch, and Grafana lets you in anonymously as an admin with no
login form. That looks alarming written down, so here's the reasoning. The only
wall that matters is the one at the front of the building -- Cloudflare Access in
front of cloudflared. Past that wall, the fleet already trusts itself: kafka and
schema-registry speak plaintext to each other for the same reason. So this stack
stores **no credentials** -- not because we forgot, but because adding a second
wall behind the first one would only be theater. Real auth on the data plane is a
deliberate later change, the same one that's pending for kafka.

## What actually fills the shelves today

Logstash runs the stock pipeline: anything shipped to `logstash:5044` (Beats /
lumberjack) lands in the `fleet-logs-*` daily indices and is immediately
readable in Kibana. Grafana opens to an Elasticsearch datasource already wired to
those same indices, so the dashboards have something to draw the moment it boots.

## The shelf we haven't built yet

Here's the honest gap. The fleet's busiest talker is Kafka, and Logstash does
**not** listen to it. Kafka doesn't speak in plain sentences -- it carries
Confluent-framed Protobuf (a schema-registry header byte, a schema id, then the
binary payload), and stock Logstash can't read that. Teaching it to would mean
the Protobuf codec plugin, compiled message descriptors, and a registry-aware
deframer -- a build, not a config tweak. So it's a tracked follow-up, not
something quietly half-wired here. Until that shelf exists, the Kafka -> store
path stays with `obs-svc-agg`, which already speaks the Protobuf contracts
fluently.

## Standing it up

The images are already in the local docker daemon, so this costs no network.
Load them into the k3d node (a host -> cluster copy) and apply:

```sh
for img in \
  docker.elastic.co/elasticsearch/elasticsearch:8.15.3 \
  docker.elastic.co/kibana/kibana:8.15.3 \
  docker.elastic.co/logstash/logstash:8.15.3 \
  grafana/grafana:11.3.0; do
  docker save "$img" | docker exec -i k3d-fleet-server-0 ctr -n k8s.io images import -
done

kubectl apply -k kube/elk/
```

> A note from experience: `k3d image import` is the documented way to do that
> load, and on this setup it reported success while quietly importing nothing.
> The `docker save | ctr import` form above is the one that actually lands the
> image in the node's containerd. Check with
> `docker exec k3d-fleet-server-0 ctr -n k8s.io images ls | grep elastic`
> before assuming a pull succeeded.

Check the manifests without touching the cluster:

```sh
kubectl apply --dry-run=client -k kube/elk/
```

## Walking in

Kibana and Grafana answer at `kibana.local` and `grafana.local` through traefik;
resolve those to the cluster edge the way the other `*.local` fleet hosts are
reached. Or skip the edge entirely and port-forward:

```sh
kubectl -n fleet port-forward svc/kibana 5601:5601    # http://localhost:5601
kubectl -n fleet port-forward svc/grafana 3000:3000   # http://localhost:3000
```
