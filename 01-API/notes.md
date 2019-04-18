# API

## Introduction

### What are APIs and why do we use them?

- APIs allow you to interact with other programs problematically 
- May be limited
- Copy and paste is an API on your OS

### RSS

- XML-based vocabulary for distributing web content. Allow the user to have content delivered as soon as it's published. 
- Different versions of RSS exist, mostly the same, but good to be aware
- iTunes aggregates pocasts with RSS
- RSS is an API

Example:

```html
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom" >
  <channel >
    <title>New Releases This Week</title>
    <ttl>10080</ttl>
    <link>http://dvd.netflix.com</link>
    <description>New movies at Netflix this week</description>
    <language>en-us</language>
    <cf:treatAs xmlns:cf="http://www.microsoft.com/schemas/rss/core/2005">list</cf:treatAs>
    <atom:link href="http://dvd.netflix.com/NewReleasesRSS" rel="self" type="application/rss+xml"/>
    <item>
      <title>Bakery in Brooklyn</title>
      <link>https://dvd.netflix.com/Movie/Bakery-in-Brooklyn/80152426</link>
      <guid isPermaLink="true">https://dvd.netflix.com/Movie/Bakery-in-Brooklyn/80152426</guid>
      <description>&lt;a href=&quot;https://dvd.netflix.com/Movie/Bakery-in-Brooklyn/80152426&quot;&gt;&lt;img src=&quot;//secure.netflix.com/us/boxshots/small/80152426.jpg&quot;/&gt;&lt;/a&gt;&lt;br&gt;Vivien and Chloe have just inherited their Aunt's bakery, a boulangerie that has been a cornerstone of the neighborhood for years. Chloe wants a new image and product, while Vivien wants to make sure nothing changes. Their clash of ideas leads to a peculiar solution, they split the shop in half. But Vivien and Chloe will have to learn to overcome their differences in order to save the bakery and everything that truly matters in their lives.</description>
    </item>
```

### How do you access API/RSS?

Most commonly, you are looking at a REST API.
- You provide a URL and the API returns structured data
- As a user, you construct the URL and send to the program
- URLs contain a path and a query at the end
- Sometimes, you need a key, which is included in the query
- Data returned most often as JSON (more common) or XML
- Libraries in python let us easily work with JSON and XML

### How do you store your API keys?

I like the recommendations [here](http://blog.revolutionanalytics.com/2015/11/how-to-store-and-use-authentication-details-with-r.html) and [here](http://www.blacktechdiva.com/hide-api-keys/). Pick the method that works best given your situation. 

## Tutorials

### RSS

We'll use the feedparser package to convert the RSS feed into a python dict

```python 
import feedparser
import pandas as pd

RSS_URL = "http://dvd.netflix.com/Top100RSS"
feed = feedparser.parse(RSS_URL)
type(feed)
```

feed is a dictionary, and you can perform normal dictionary operations on it. Not too much else new here - the .get() function seems useful when working with dicts and I didn't know about it.

`dict.get(key[, value])`

The get() method returns the value for the specified key if key is in dictionary. The get() method takes maximum of two parameters:
- key - key to be searched in the dictionary
- value (optional) - Value to be returned if the key is not found. The default value is None.

When looping through an RSS feed, this is a way to handle errors if the feed is imcomplete / lacking some information.


### API