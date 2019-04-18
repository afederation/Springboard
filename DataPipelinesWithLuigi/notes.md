# Building Data Pipelines

What is it? A python package that helps build a complex pipeline of batch jobs. Useful if you have a somewhat complex batch job that needs to be run over and over.

A tree of discrete tasks that you create dependencies between.

Already has built in modules for a lot of common tools - mySQL, postgreSQL, hadoop, hive, spark, etc.

## 3 Magic Methods

1. run
2. output
3. requires

**Example:**

some_task.py
```python
class SomeTask(luigi.Task):
    def run(self):
        #code that actually does the work
    def output(self):
        return [SomeTarget()]
        # Is this step finished?
        # Target implements an exists() method, which returns True (finished) or Fails
    def requires(self):
        return [AnotherTask]
        # Return a list of the next set of tasks
```

How to run it:
run_my_luigi_task.py

```sh
#!/bin/sh
luigiid --background --port=8082

python some_task.py SomeTask
```

For a test pipeline, check out the hello_world directory.


## Parameterized Jobs

In the first example, the pipeline would only run if the output files don't exist. How can you get it to run over and over?

Add a luigi.parameter to all of the Tasks that will create a new directory when you pass a parameter through this. Parameters are the Luigi equivalent of creating a constructor (normally defined in __init__) for each Task. Luigi requires you to declare these parameters by instantiating Parameter objects on the class scope.

```python
class HelloWorldTask(luigi.Task):
    id = luigi.Parameter(default = 'test')    
```

Then it's just a matter of changing all the paths to use this new paramater. A note - in the HelloWorldTask top level Task, we can use the self.input and then give it an index to refer to elements that are defined in the requires() function.

```python
    def run(self): 

        with open(self.input()[0].path, 'r') as hello_file:     #refers to the first element in the requires function 
            hello = hello_file.read()                            
```


## Making Directories

So, this mostly works, except the script throws an error because the nessecary directories don't exist. To solve this, we can make a new luigi.Task to create the directories, then make the HelloTask and the WorldTask both require this task to be complete.

```python
    def requires(self):
        return[MakeDirectory(path=os.path.dirname(self.path))]
```

os.path.dirname is something I haven't used before - it just returns the path (not filename) of a path.

Now it runs.

## Configuring the pipeline

You can create a luigi.cfg file that can configure how the job is run. The IP address lets us put the luigi daemon on a different server (like AWS) Example:

```sh
[core].  #just a comment to define a section?
default-scheduler-host: 123.45.67.89

[email]
reciever: admin@example.com
method: ses   #credentials in AWS config file
```

You can also have a staging and production pipeline

```sh
export LUIGI_CONFIG_PATH=staging.luigi.cfg
python data_pipeline AllData --id='123'
```

Settings you define in the cfg file can be used as a Luigi class. Continuing with the example where you have a staging and production setup:


./staging.luigi.cfg
```sh
[s3File]
bucket: my-bucket-staging
key: path/to/file1
```
./production.luigi.cfg
```sh
[s3File]
bucket: my-bucket-production
key: path/to/file2
```

some_task.py
```python
class s3File(luigi.Config):
    bucket = luigi.Parameter()
    key = luigi.Parameter()

class SomeTask(luigi.Task):

    def output(self):

        return luigi.contrib.s3.S3Target(.      #using built-in support for S3
            path='s3://{}/{}.format'(
                s3File().bucket, s3File.key()
                )
            )
```

## Exercise - Create a Data Pipeline

You have two types of log files in a bucker in Amazon S3:
1. UserUpdates
2. PageViews

You want to create a table in a SQL database:
1. user_page_views