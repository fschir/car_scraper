#Car-Scraper

A simple scraper which enables you to scrape Autoscout24.de

##Getting started

Just download python set up mongodb and oyu are good to go.
To avoid duplicates in your DB  make sure to set the sha1 key
as unique.

db.cars.createIndex( { "sha1": 1 }, { unique: true, sparse: true } )

You're good to go.

###Requirements

MongoDB
scrapy
pymongo