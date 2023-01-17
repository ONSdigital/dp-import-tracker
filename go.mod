module github.com/ONSdigital/dp-import-tracker

go 1.19

// to avoid 'sonatype-2021-4899' non-CVE Vulnerability
exclude github.com/gorilla/sessions v1.2.1

// to avoid the following vulnerabilities:
//	- CVE-2022-29153	pkg:golang/github.com/hashicorp/consul/api@v1.1.0
//	- sonatype-2021-1401	pkg:golang/github.com/miekg/dns@v1.0.14
//	- sonatype-2019-0890	pkg:golang/github.com/pkg/sftp@v1.10.1
replace github.com/spf13/cobra => github.com/spf13/cobra v1.6.1

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.192.0
	github.com/ONSdigital/dp-graph/v2 v2.15.0
	github.com/ONSdigital/dp-healthcheck v1.5.0
	github.com/ONSdigital/dp-import v1.3.1
	github.com/ONSdigital/dp-kafka/v2 v2.7.3
	github.com/ONSdigital/dp-net/v2 v2.6.0
	github.com/ONSdigital/log.go/v2 v2.3.0
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.15.12 // indirect
	github.com/smartystreets/goconvey v1.7.2
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/net v0.5.0 // indirect
	golang.org/x/sys v0.4.0 // indirect
)

require (
	github.com/ONSdigital/dp-api-clients-go v1.43.0 // indirect
	github.com/ONSdigital/dp-net v1.5.0 // indirect
	github.com/ONSdigital/golang-neo4j-bolt-driver v0.0.0-20210408132126-c2323ff08bf1 // indirect
	github.com/ONSdigital/graphson v0.2.0 // indirect
	github.com/ONSdigital/gremgo-neptune v1.0.2 // indirect
	github.com/Shopify/sarama v1.37.2 // indirect
	github.com/aws/aws-sdk-go v1.44.124 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11 // indirect
	github.com/gofrs/uuid v4.3.0+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hokaccha/go-prettyjson v0.0.0-20211117102719-0474bc63780f // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.3 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/justinas/alice v1.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/smartystreets/assertions v1.13.0 // indirect
)
