import os
import luigi
from time import sleep

class MakeDirectory(luigi.Task):

    path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.path)

    def run(self):
        os.makedirs(self.path)

class HelloTask(luigi.Task):
    path = luigi.Parameter()


    def run(self):
        sleep(10)
        with open(self.path, 'w') as hello_file:

            '''
            benefit of a with statment:
            no matter what, the file will close after this code block
            '''

            hello_file.write('Hello')
            hello_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return[MakeDirectory(path=os.path.dirname(self.path))]

class WorldTask(luigi.Task):
    path = luigi.Parameter()

    def run(self):
        sleep(5)
        with open(self.path, 'w') as world_file:

            '''
            benefit of a with statment:
            no matter what, the file will close after this code block
            '''

            world_file.write('World')
            world_file.close()

    def output(self):
        return luigi.LocalTarget(self.path)

    def requires(self):
        return[MakeDirectory(path=os.path.dirname(self.path))]


class HelloWorldTask(luigi.Task):
    pid = luigi.Parameter(default = 'test')    

    def run(self): 

        with open(self.input()[0].path, 'r') as hello_file:     #refers to the first element in the requires function 
            hello = hello_file.read()                            
        with open(self.input()[1].path, 'r') as world_file:     #refers to the first element in the requires function
            world = world_file.read()
        with open(self.output().path, 'w') as output_file:
            content = f'{hello} {world}'
            output_file.write(content)
            output_file.close()

    def output(self):
        path = f'./results/{self.pid}/hello_world.txt'
        return luigi.LocalTarget(path)

    def requires(self):
        return [HelloTask(
            f'./results/{self.pid}/hello.txt'
            ), 
                WorldTask(
            f'./results/{self.pid}/world.txt'
            )
                ]

if __name__ == '__main__':
    luigi.run()

