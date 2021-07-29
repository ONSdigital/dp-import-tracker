module github.com/ONSdigital/dp-import-tracker

go 1.16

replace github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible

require (
	github.com/ONSdigital/dp-api-clients-go v1.41.1 // indirect
	github.com/ONSdigital/dp-api-clients-go/v2 v2.1.7-beta
	github.com/ONSdigital/dp-graph/v2 v2.13.1
	github.com/ONSdigital/dp-healthcheck v1.1.0
	github.com/ONSdigital/dp-import v1.1.0
	github.com/ONSdigital/dp-kafka/v2 v2.3.1
	github.com/ONSdigital/dp-net v1.0.12
	github.com/ONSdigital/log.go v1.1.0
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20210722203344-69c5ea87048d // indirect
	github.com/gorilla/mux v1.8.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/smartystreets/goconvey v1.6.4
	golang.org/x/crypto v0.0.0-20210711020723-a769d52b0f97 // indirect
	golang.org/x/net v0.0.0-20210726213435-c6fcb2dbf985 // indirect
)
