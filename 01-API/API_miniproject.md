
This exercise will require you to pull some data from the Qunadl API. Qaundl is currently the most widely used aggregator of financial market data.

As a first step, you will need to register a free account on the http://www.quandl.com website.

After you register, you will be provided with a unique API key, that you should store:


```python
# Store the API key as a string - according to PEP8, constants are always named in all upper case
API_KEY = 'xxx'
```

Qaundl has a large number of data sources, but, unfortunately, most of them require a Premium subscription. Still, there are also a good number of free datasets.

For this mini project, we will focus on equities data from the Frankfurt Stock Exhange (FSE), which is available for free. We'll try and analyze the stock prices of a company called Carl Zeiss Meditec, which manufactures tools for eye examinations, as well as medical lasers for laser eye surgery: https://www.zeiss.com/meditec/int/home.html. The company is listed under the stock ticker AFX_X.

You can find the detailed Quandl API instructions here: https://docs.quandl.com/docs/time-series

While there is a dedicated Python package for connecting to the Quandl API, we would prefer that you use the *requests* package, which can be easily downloaded using *pip* or *conda*. You can find the documentation for the package here: http://docs.python-requests.org/en/master/ 

Finally, apart from the *requests* package, you are encouraged to not use any third party Python packages, such as *pandas*, and instead focus on what's available in the Python Standard Library (the *collections* module might come in handy: https://pymotw.com/3/collections/).
Also, since you won't have access to DataFrames, you are encouraged to us Python's native data structures - preferably dictionaries, though some questions can also be answered using lists.
You can read more on these data structures here: https://docs.python.org/3/tutorial/datastructures.html

Keep in mind that the JSON responses you will be getting from the API map almost one-to-one to Python's dictionaries. Unfortunately, they can be very nested, so make sure you read up on indexing dictionaries in the documentation provided above.


```python
import requests
import json
import pandas as pd

```


```python
# Now, call the Quandl API and pull out a small sample of the data (only one day) to get a glimpse
# into the JSON structure that will be returned

database_code = 'FSE'
dataset_code = 'AFX_X'
return_format = 'json'
start_date = '2019-04-01' 
end_date = '2019-04-01'

url = 'https://www.quandl.com/api/v3/datasets/%s/%s/data.%s' % (database_code, dataset_code, return_format)

params =  {'api_key' : API_KEY, 'start_date' : start_date, 'end_date': end_date} 

r = requests.get(url, params=params)

```


```python
# Inspect the JSON structure of the object you created, and take note of how nested it is,
# as well as the overall structure

print(r.url)
print(r.status_code)

r.json()
```

    https://www.quandl.com/api/v3/datasets/FSE/AFX_X/data.json?api_key=Gfi-DJ4xkDLm2TPfXxAS&start_date=2019-04-01&end_date=2019-04-01
    200





    {'dataset_data': {'limit': None,
      'transform': None,
      'column_index': None,
      'column_names': ['Date',
       'Open',
       'High',
       'Low',
       'Close',
       'Change',
       'Traded Volume',
       'Turnover',
       'Last Price of the Day',
       'Daily Traded Units',
       'Daily Turnover'],
      'start_date': '2019-04-01',
      'end_date': '2019-04-01',
      'frequency': 'daily',
      'data': [['2019-04-01',
        None,
        76.0,
        74.1,
        74.9,
        None,
        186845.0,
        14041454.0,
        None,
        None,
        None]],
      'collapse': None,
      'order': None}}



These are your tasks for this mini project:

1. Collect data from the Franfurt Stock Exchange, for the ticker AFX_X, for the whole year 2017 (keep in mind that the date format is YYYY-MM-DD).



```python
database_code = 'FSE'
dataset_code = 'AFX_X'
return_format = 'json'
start_date = '2017-01-01' 
end_date = '2017-12-31'

url = 'https://www.quandl.com/api/v3/datasets/%s/%s/data.%s' % (database_code, dataset_code, return_format)

params =  {'api_key' : API_KEY, 'start_date' : start_date, 'end_date': end_date} 

r = requests.get(url, params=params)

print(r.url)
print(r.status_code)
```

    https://www.quandl.com/api/v3/datasets/FSE/AFX_X/data.json?api_key=Gfi-DJ4xkDLm2TPfXxAS&start_date=2017-01-01&end_date=2017-12-31
    200


2. Convert the returned JSON object into a Python dictionary.



```python
afx_dict = r.json()
type(afx_dict)

```




    dict



3. Calculate what the highest and lowest opening prices were for the stock in this period.



```python

afx_df = pd.DataFrame.from_dict(afx_dict['dataset_data']['data'])
afx_df.columns = afx_dict['dataset_data']['column_names']                     
print("Max:", afx_df['Open'].max())
print("Min:", afx_df['Open'].min())
```

    Max: 53.11
    Min: 34.0


4. What was the largest change in any one day (based on High and Low price)?



```python
afx_df.head
delta = afx_df['High'] - afx_df['Low']
print("Largest Change:", delta.max())
```

    Largest Change: 2.8100000000000023


5. What was the largest change between any two days (based on Closing Price)?



```python
change = []
for index in range(len(afx_df.index)-1):
    change.append(afx_df.iloc[index, 4] - afx_df.iloc[index+1, 4])
print("Max DTD Change:", max(change))
```

    Max DTD Change: 1.7199999999999989


6. What was the average daily trading volume during this year?



```python
print("Average Daily Trading Volume:", afx_df['Traded Volume'].mean())
```

    Average Daily Trading Volume: 89124.33725490196


7. (Optional) What was the median trading volume during this year. (Note: you may need to implement your own function for calculating the median.)


```python
print("Median Daily Trading Volume:", afx_df['Traded Volume'].median())
    # why do they suggest a custom function here?
```

    Median Daily Trading Volume: 76286.0

