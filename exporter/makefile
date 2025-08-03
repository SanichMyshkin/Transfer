push:
	docker build -t nexus-metrics .
	docker tag nexus-metrics sanichmyshkin/nexus-metrics:latest
	docker push sanichmyshkin/nexus-metrics:latest

build:
	docker build -t nexus-metrics .

stop:
	docker stop nexus-metrics
	docker rm nexus-metrics
	docker rmi nexus-metrics

l:
	docker logs nexus-metrics