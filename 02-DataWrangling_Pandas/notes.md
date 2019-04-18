# Data Wrangling with Pandas

## Introduction
[Source](https://github.com/talumbau/strata_data/blob/master/strata_pandas.ipynb)

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

This uses the 'Date' column as the indexes for the rows. Default parse_dates is False, so be aware.

Pandas DataReader is a nifty extension that is already hooked up to some common data sources, mostly financial data.

```python
from pandas_datareader import data as pd_data

start  = pd.Timestamp('2010-1-1')
end    = pd.Timestamp('2014-12-31')
google = pd_data.DataReader("GOOG", 'google', start, end)

##Then you can convert to a csv, which is the file that was imported above
google.to_csv('data/goog.csv', index=True, encoding='utf-8')
```

Glob is a nice tool to do unix-style regular expressions to grab lists of files
```Python
# Write the pattern: pattern
pattern = '*.csv'

# Save all file matches: csv_files
csv_files = glob.glob(pattern) # Gives a list of all CSV files in the directory

# Then you can iterate through the files and cat them
# Create an empty list: frames
frames = []

#  Iterate over csv_files
for csv in csv_files:

    #  Read csv into a DataFrame: df
    df = pd.read_csv(csv)

    # Append df to frames
    frames.append(df)

# Concatenate frames into a single DataFrame: uber
uber = pd.concat(frames)
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
Check what types the data are in

`df.dtypes()`

To get a column by index, you can use dict notation or dot notation

`google['High']` or `google.High`

To index by numbers, you need to use the iloc method.

`google['Open'].iloc[:5]` Gives the first 5 values of the Open column.

To index by the strings, use .loc. Can also index by dates, which is pretty awesome. Same operation with dates:

`google.loc['2010-01-04':'2010-01-08','Open']`

Iteration
`for key, value in series2.iteritems():
    print(key, value)`

You can sort by the indexes
`data = data.set_index(data.index.sort_values(ascending=False))`

Categorial data

`df['sex'] = df['sex'].astype('category')`

The categorical data type is useful in the following cases:
- A string variable consisting of only a few different values. Converting such a string variable to a categorical variable will save some memory.
- The lexical order of a variable is not the same as the logical order (“one”, “two”, “three”). By converting to a categorical and specifying an order on the categories, sorting and min/max will use the logical order instead of the lexical order.
- As a signal to other python libraries that this column should be treated as a categorical variable
e.g. to use suitable statistical methods or plot types.

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

To get a DataFrame from a DataFrame, index by a list of columns
`df_col = df_grades[['Alice']]`
Or for a row:
`df_row = df_grades.loc['Jan':'Jan']`

Adding columns
- zero fill:  `df['var'] = 0`
- values from NumPy array: df['my_data'] = data
- note: df.var construct can not create a column by that name; only used to access existing columns by name

Deleting columns
`del data['FIRSTURL']`

Renaming columns
`data = data.rename(columns={'NAME':'PLANET'})`

## Data Cleaning

You can cooerce a data transformation, like for example numbers that were loaded as strings.

`tips['total_bill'] = pd.to_numeric(tips['total_bill'], errors='coerce')`

You can write custom functions and apply them across columns (axis=0) or rows(axix=1). Example:

```Python
def diff_money(row, pattern):
 icost = row['Initial Cost']
 tef = row['Total Est. Fee']

 if bool(pattern.match(icost)) and bool(pattern.match(tef)):
   icost = icost.replace("$","")
   tef = tef.replace("$","")

   icost = float(icost)
   tef = float(tef)

   return icost - tef
else:
   return(NaN)

pattern = re.compile('^\$\d*\.\d{2}$')
df_subset['diff'] = df_subset.apply(diff_money,
                                    axis=1,
                                    pattern=pattern) #explicitly define the parameters of the function
 ```

Lambda functions make this more efficient.

`tips['total_dollar_replace'] = tips.total_dollar.apply(lambda x: x.replace('$', ''))`

You can remove duplicates with `df.drop_duplicates()`

Drop NA values with `df.dropna()`
Replace NA values with `tips_nan.fillna(<value to replace with>)`

To check your cleaning, you can use Assert statements.
- assert 1 == 1; returns nothing
- assert 1 == 2; returns as AssertionError
You can check the make sure you don't have any null values.
`assert google.Close.notnull().all()`
Note: the .all() returns True if all elements are True.

To check for the entire table, you need two .all() statements - the first one for the columns, the second for the rows.



## Turning Tuesday

Cool example. Found [here](https://github.com/talumbau/strata_data/blob/master/Turning_Tuesday_PyData.ipynb)

Nothing new on the code, really.
