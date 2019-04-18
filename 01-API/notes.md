# API

## Introduction

### What are APIs and why do we use them?

- APIs allow you to interact with other programs problematically
- May be limited
- Copy and paste is an API on your OS

### RSS

- XML-based vocabulary for distributing web content. Allow the user to have content delivered as soon as it's published.
- Different versions of RSS exist, mostly the same, but good to be aware
- iTunes aggregates podcasts with RSS
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

## Web Scraping

- Process of directly accessing a page and copying data when an API is not available.
- Different than **web crawling**, which gathers and traverses links
- Often crawling and scraping are used in conjunction with each other.
- It can violate the terms of service of a website, can have legal repercussions
- Even if not illegal, can be ethically ambiguous (can look like a DOS attach)
- Mostly have to write new code for every site

### David Eads


"IF YOU NEED A SCRAPER, YOU HAVE A DATA PROBLEM."
David Eads, PyData DC, 10/9/16
[Useful Scraping Techniques](http://blog.apps.npr.org/2016/06/17/scraping-tips.html)
When I first tried web scraping, a friend sent me a link to a blog post that David Eads wrote about a scraper he built that is pretty amazing. He also gave a good talk about scraping at the last PyData DC. He works at NPR on the visuals team.

"Fundamentally, a scraper means something isn’t right in the world. There’s a database out there, somewhere, powering a website, but you only have access to the website.

But the good data… well, if it was easy to get, it probably wouldn’t be the good shit."

**NEVER SCRAPE UNLESS YOU...**
- have no other way of liberating the data
- budget appropriately
- consider the ethical ramifications (do some googling and read about the ethics)
- read terms of service and do your legal research
- talk to a lawyer (if you possibly can)

If time allows, try FOIA. Scraping can turn into a denial of service attack.

### robots.txt

Tells you how to crawl their data.  Doesn't force you to do it though.

*The robots exclusion standard, also known as the robots exclusion protocol or simply robots.txt, is a standard used by websites to communicate with web crawlers and other web robots. The standard specifies how to inform the web robot about which areas of the website should not be processed or scanned. Robots are often used by search engines to categorize web sites. Not all robots cooperate with the standard; email harvesters, spambots, malware, and robots that scan for security vulnerabilities may even start with the portions of the website where they have been told to stay out. The standard is different from, but can be used in conjunction with, Sitemaps, a robot inclusion standard for websites.*

### Python

There are a lot of libraries available in Python to help with this task:

- urllib2 - module for processing urls, expanded in Python 3
- requests - improved upon urllib/ urllib2
- lxml - extensive library for parsing XML and HTML, some people find it a little more difficult to use initially
- beautifulsoup4 - library for pulling data out of HTML and XML files

There is usually more than one way to do things in Python. What you end up with tends to be what you learn first and/ or what you are most comfortable with.

- Scrapy - application framework for scraping
- Nutch - extensible and scalable open source web crawler software project


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

Using json and urllib. Requests library is also an option to replace urllib, may be more robust?

```python
import json
import urllib.request


data = json.loads(urllib.request.urlopen('http://www.omdbapi.com/?t=Game%20of%20Thrones&Season=1').read().\
                  decode('utf8'))
        # UTF-8 helps deail with any coding issues on import
```

Like before, also returns a dictionary. Easy to deal with.

What if we need an API key!? Depends on the site, for this exmaple, you can use a command-line request to get a key.

It's good practice not to put your keys in your code. Store your API key as a JSON file in the repo, then .gitignore it. Here's an example:

```python
with open("./dpla_config_secret.json") as key_file:
    key = json.load(key_file)

import requests

# we are specifying our url and parameters here as variables
url = 'http://api.dp.la/v2/items/'
params = {'api_key' : key['api_key'], 'q' : 'goats+AND+cats'} # q is the query
r = requests.get(url, params=params)
```

Good for troubleshooting, you can edconstruct your call

```python
# we can look at the url that was created by requests with our specified variables
r.url

# we can check the status code of our request
r.status_code

# we can look at the content of our request
print(r.content)
```

Returns a dictionary again.

### Scraping

Use urllib to pull the html and scrape it.

```python

import urllib

html = urllib.urlopen("http://xkcd.com/1481/")
print(html.read())
```

Make sure we're not violating the robots.txt file

```python
robot = urllib.urlopen("https://xkcd.com/robots.txt")
print(robot.read())
```

We can use the urlretrieve function to retrieve a specific resources, such as a file, via url. This is basic web scraping.

If we look through our html above, we can see there is a url for the image in the page. (Look for: Image URL (for hotlinking/embedding): https://imgs.xkcd.com/comics/api.png)


```python
urllib.urlretrieve("http://imgs.xkcd.com/comics/api.png", "api.png")`
```

Using these methods, we are treating the html as an unstructured string. If we want to retrieve the structured markup, we can use BeautifulSoup. *Beautiful Soup is a Python library for pulling data out of HTML and XML files. It works with your favorite parser to provide idiomatic ways of navigating, searching, and modifying the parse tree. It commonly saves programmers hours or days of work.*

```python
from bs4 import BeautifulSoup
url = "https://litemind.com/best-famous-quotes"

html = urllib.urlopen(url).read()
soup = BeautifulSoup(html,"html.parser")
print(soup.prettify())
```

If I'm going to use this, I'll dive into the docs.

`soup.title` gives you the title tag, for example. `soup.title.string` gives you the string within the <title> tags.

Can do `soup.head` and `soup.body`, etc.
