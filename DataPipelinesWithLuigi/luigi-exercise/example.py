import csv
import os

import luigi
import luigi.contrib.s3
import luigi.contrib.postgres


# Configuration classes
class postgresTable(luigi.Config):
    host = luigi.Parameter()
    password = luigi.Parameter()
    database = luigi.Parameter()
    user = luigi.Parameter()


class s3Bucket(luigi.Config):
    key = luigi.Parameter()
    secret = luigi.Parameter()


# Tasks
class GetUserUpdatesFromS3(luigi.Task):
    id = luigi.Parameter()
    extension = '.csv'
    s3_directory = 's3://pycon-2017-luigi-presentation-data/UserUpdates/'
    local_directory = './downloads/UserUpdates/'

    @property
    def client(self):
        key = s3Bucket().key
        secret = s3Bucket().secret

        return luigi.contrib.s3.S3Client(aws_access_key_id=key, aws_secret_access_key=secret)

    @property
    def s3_files(self):
        return [s3_file for s3_file in self.client.listdir(self.s3_directory) if s3_file.endswith(self.extension)]

    def get_local_file_name(self, s3_file_name):
        return s3_file_name.replace(self.s3_directory, '{}{}_'.format(self.local_directory, self.id))

    @property
    def local_files(self):
        return [self.get_local_file_name(file) for file in self.s3_files]

    def run(self):
        for s3_file in self.s3_files:
            self.client.get(s3_file, self.get_local_file_name(s3_file))

    def output(self):
        return [luigi.LocalTarget(file) for file in self.local_files]


class ConcatenateUserUpdates(luigi.Task):
    id = luigi.Parameter()

    def run(self):
        os.system('cat ./downloads/UserUpdates/{0}_*.csv >> generated_files/{0}_user_updates.csv'.format(self.id))

    def output(self):
        return luigi.LocalTarget('generated_files/{}_user_updates.csv'.format(self.id))

    def requires(self):
        return [GetUserUpdatesFromS3(id=self.id)]


class WriteUserUpdatesToSQL(luigi.contrib.postgres.CopyToTable):
    id = luigi.Parameter()
    host = postgresTable().host
    password = postgresTable().password
    database = postgresTable().database
    user = postgresTable().user
    table = 'user_updates'

    @property
    def update_id(self):
        return '{}_{}'.format(self.table, self.id)

    columns = [
        ('ts', 'timestamp'),
        ('user_id', 'int'),
        ('email', 'text'),
        ('name', 'text'),
        ('environment', 'text')
    ]

    @property
    def source_csv(self):
        return './generated_files/{}_user_updates.csv'.format(self.id)

    def rows(self):
        with open(self.source_csv, 'r') as csv_file:
            reader = csv.reader(csv_file)
            rows = [row for row in reader if len(row) == len(self.columns)]
            return rows

    def requires(self):
        return [ConcatenateUserUpdates(id=self.id)]


if __name__ == '__main__':
    luigi.run()
