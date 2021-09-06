module github.com/ONSdigital/dp-import-tracker

go 1.16

replace github.com/coreos/etcd => github.com/coreos/etcd v3.3.24+incompatible

require (
	github.com/ONSdigital/dp-api-clients-go v1.43.0 // indirect
	github.com/ONSdigital/dp-api-clients-go/v2 v2.1.14
	github.com/ONSdigital/dp-graph/v2 v2.14.0
	github.com/ONSdigital/dp-healthcheck v1.1.1
	github.com/ONSdigital/dp-import v1.2.1
	github.com/ONSdigital/dp-kafka/v2 v2.4.1
	github.com/ONSdigital/dp-net v1.2.0
	github.com/ONSdigital/log.go v1.1.0
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gorilla/mux v1.8.0
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/klauspost/compress v1.13.5 // indirect
	github.com/smartystreets/goconvey v1.6.4
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5 // indirect
	golang.org/x/net v0.0.0-20210903162142-ad29c8ab022f // indirect
	golang.org/x/sys v0.0.0-20210903071746-97244b99971b // indirect
)
