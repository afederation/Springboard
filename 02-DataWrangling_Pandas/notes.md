# Data Wrangling with Pandas

## Introduction

Pandas is built for doing practical data analysis using Python
- Used extensively in production in financial applications.
- Well-suited for tabular data (e.g. column data, spread-sheets, databases)
- Can replace or integrate with Excel and SQL queries.


Pandas fills a gap in the Python scientific eco-system:
- Convenient methods for reading/writing a large variety of file formats, e.g. Excel, HDF, SQL, CSV, etc.
- Meaningful labels for quantitative and categorical data
- Easy handling missing data and NaN (much harder to do with numpy arrays alone).
- SQL-like methods for manipulating tabular data, such as pivot, groupby, merge/join.
- Convenient methods for routine mathematical and summary statistics

## Pandas Structures

There are two general data types in Pandas, Series and DataFrame.
1. A series is essentially a numpy array, a 1-D type of data. Must contain all the same data type
2. The DataFrame is a 2-D heterogeneous data type comprised of multiple Series acting as the columns.

Each of these data types contain an **Index**.
1. In a Series, the index is a second array that also contains homogeneous data, can be numbers or strings
2. In a DataFrame, objects include a column index.

### Integration

Pandas is tightly integrated with the rest of the scientific Python ecosystem
- built on Numpy arrays and ufuncs
- pandas data structures can be passed into numpy, matplotlib, and bokeh methods
- has built-in visualization using Matplotlib
- a dependency for Python statistics library statsmodel
- a NumFOCUS supported project

## Data Import

Import is pretty simple:

`google = pd.read_csv('data/goog.csv', index_col='Date', parse_dates=True)`

This uses the 'Date' column as the indexes for the rows.

Pandas DataReader is a nifty extension that is already hooked up to some common data sources, mostly financial data.

```python
from pandas_datareader import data as pd_data

start  = pd.Timestamp('2010-1-1')
end    = pd.Timestamp('2014-12-31')
google = pd_data.DataReader("GOOG", 'google', start, end)

##Then you can convert to a csv, which is the file that was imported above
google.to_csv('data/goog.csv', index=True, encoding='utf-8')
```

## Data Access and Inspection

**(Almost) All the complexity in pandas (and it is actually, somewhat complex in parts) arises from the ability to index by POSITION (e.g. 0, 1, 2, ith location) and also index by LABEL ('a', 'b', 'c', etc.).**

Some common, obvious methods here.
```python
google.head()
google.tail()
google.shape
google.info() #gives you column name, datatype, #values
google.describe() #summary statistics
```

To get a column by index, you can use dict notation or dot notation

`google['High']` or `google.High`

To index by numbers, you need to use the iloc method.

`google['Open'].iloc[:5]` Gives the first 5 values of the Open column.

To index by the strings, use .loc. Can also index by dates, which is pretty awesome. Same operation with dates:

`google.loc['2010-01-04':'2010-01-08','Open']`

Iteration
`for key, value in series2.iteritems():
    print(key, value)`

## Data Filtering

You can filter by conditional statements

```python
google_up = google[ google['Close'] > google['Open'] ]

google_filtered = google[pd.isnull(google['Volume']) == False ] #filter out missing data
```

An awesome built-in is this **.pct_change**. Gives the % change between two adjacent rows.

`google['Return'] = google['Close'].pct_change()`

## Data Manipulation

Most arithmetic is standard. a cool feature is if you have data with matching indices. For example, here addition will work through the index, not through the position in the series. Acts as an SQL outer join

```python
series1 = pd.Series([1,2,3,4,5],      index=['a','b','c','d','e'])
series2 = pd.Series([10,20,30,40,50], index=['c','d','e','f','g'])

print( series1 + series2 )
```

```
a     NaN
b     NaN
c    13.0
d    24.0
e    35.0
f     NaN
g     NaN
dtype: float64
```
