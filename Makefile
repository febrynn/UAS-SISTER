# Makefile for managing the uts-event-system project

.PHONY: build aggregator publisher up down logs test

build: 
	docker-compose build

aggregator: 
	docker-compose up -d aggregator

publisher: 
	docker-compose up -d publisher

up: 
	docker-compose up -d

down: 
	docker-compose down

logs: 
	docker-compose logs -f

test: 
	docker-compose run --rm aggregator pytest
	docker-compose run --rm publisher pytest
	docker-compose run --rm tests pytest