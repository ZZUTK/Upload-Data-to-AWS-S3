from os.path import basename, join, isfile, isdir, realpath, expanduser
from glob import glob
import threading
from time import sleep, time
from os import walk
try:
    import queue
except (ModuleNotFoundError, ImportError):
    import Queue as queue
try:
    import boto3
    import boto3.session
except (ModuleNotFoundError, ImportError):
    raise Exception('Require boto3, please refer to '
                    'https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html')


# Go to https://klam-sj.corp.adobe.com/aws/adobeaws.php?userData=klam for the following info
aws_access_key_id='ASIAU555WZTZ6P2NRHL6'
aws_secret_access_key='Wi6fKjkpIOsRalgMFFZuscIk1jQzYnWaZgHOKqbE'
aws_session_token='FwoGZXIvYXdzELn//////////wEaDAiYatvl4t2sgFteTiLEAUVktuvmHrd4dqtN18UeDjM7CBzP/nLlf90fz67+PFqrsQmomLxpWfxOqfWkiWvSiiQSNHXdepOH1pYQ9hTfBNRwRpSVJIRT4cxsPdnpGcHKqmXiNUli8jrcucWzmtQlLtGgk9dwM8A7LwfQMm+qWigYMN7fBn7IYBciYWsEXJwyA8GLMG2cCOZWnPNK61kDXm7LtnfllKky+JfS4bG2L3kQaXYq3Xn+/MTHAj6E/9uRjguke1zz8q15jJ+d460PnVQjAt0o3PTR+QUyLVXIB+5ka6j0u+gy9uuTJfH6UM1Dr9dZUU/ERTviDxefKNtmqDJRjSrnNWm7Og=='


class ETA(object):
    # estimate remaining time
    def __init__(self, num_total=None):
        self.current_time_elapsed = None
        self.previous_time_elapsed = None
        self.num_total = num_total

    def set_total(self, num_total):
        assert isinstance(num_total, (int, float))
        self.num_total = num_total

    def __call__(self, t_start, t_end, n_start, n_end, n_total=None, sensitivity=.5):
        """
        estimate remaining time
        :param t_start: time stamp of start in second
        :param t_end: time stamp of end in second
        :param n_start: index of start
        :param n_end: index of end
        :param n_total: total samples
        :param sensitivity: float, [0, 1], higher value results in large fluctuation of time estimation
        :return: string of time remaining
        """
        if n_total is not None:
            self.num_total = n_total
        t_elapsed = t_end - t_start
        assert t_elapsed >= 0 and n_start < n_end and self.num_total is not None
        self.current_time_elapsed, self.previous_time_elapsed = t_elapsed, self.current_time_elapsed
        if self.previous_time_elapsed is None:
            self.previous_time_elapsed = self.current_time_elapsed
        self.current_time_elapsed = sensitivity * self.current_time_elapsed + (1 - sensitivity) * self.previous_time_elapsed

        # print(self.current_time_elapsed, n_start, n_end, self.num_total)

        ratio = max(float(self.num_total - n_end), 0) / (n_end - n_start)
        eta = ratio * self.current_time_elapsed
        eta_ = int(eta)

        # time formatting
        days = eta_ // (3600 * 24)
        eta_ -= days * (3600 * 24)

        hours = eta_ // 3600
        eta_ -= hours * 3600

        minutes = eta_ // 60
        eta_ -= minutes * 60

        seconds = eta_

        if days > 0:
            if days > 1:
                time_str = '%2d days %2d hr' % (days, hours)
            else:
                time_str = '%2d day %2d hr' % (days, hours)
        elif hours > 0 or minutes > 0:
            time_str = '%02d:%02d' % (hours, minutes)
        else:
            time_str = '%02d sec' % seconds

        return time_str


class UploadS3Parallel(object):
    def __init__(self, num_workers=2, max_queue_size=8, region='us-west-2'):
        self.num_workers = num_workers
        self.max_queue_size = max_queue_size
        self.queue = queue.Queue(maxsize=self.max_queue_size)
        self._stop_event = None
        self._threads = []
        self.wait_time = .1
        self.my_name = 'Thread'
        self.region = region
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )

    def check_files(self, bucket_name, num_print=float('inf')):
        if bucket_name in self.get_bucket_names():
            cnt = 0
            s3 = boto3.resource(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
            bucket = s3.Bucket(bucket_name)
            for obj in bucket.objects.all():
                print(obj)
                cnt += 1
                if cnt >= num_print:
                    break
        else:
            print('The Bucket {} does not exist!'.format(bucket_name))

    def get_bucket_names(self):
        return [bucket['Name'] for bucket in self.s3.list_buckets()['Buckets']]

    def create_bucket(self, bucket_name):
        if bucket_name in self.get_bucket_names():
            print('The Bucket {} already exist!'.format(bucket_name))
            if input('Continue to write into Bucket {}?[y/n]'.format(bucket_name)) != 'y':
                exit(0)
        else:
            self.s3.create_bucket(Bucket=bucket_name)

    def delete_bucket(self, bucket_name):
        if bucket_name in self.get_bucket_names():
            s3 = boto3.resource(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
            bucket = s3.Bucket(bucket_name)
            for obj in bucket.objects.all():
                obj.delete()
                print('Delete {}'.format(obj))
            bucket.delete()
            print('Delete Bucket {}'.format(bucket_name))
        else:
            print('The Bucket {} does not exist!'.format(bucket_name))

    def load_files(self, data_dir, regex=('*.*',)):
        assert isinstance(regex, (list, tuple)) and len(regex) > 0
        files_path = []
        for ex in regex:
            for root, subdirs, files in walk(data_dir):
                files_path.extend([_ for _ in glob(join(root, ex)) if isfile(_)])
        return files_path

    def __call__(self, data_dir, bucket_name=None, bucket_folder=None, is_public=False, regex=('*.*',)):
        """
        upload data to S3
        :param data_dir: str, dir to the data, may include subdirs
        :param bucket_name: str, name of the bucket, must be lower-case
        :param bucket_folder: str, folder path under the bucket to store data
        :param is_public: bool, whether make the data to be public (accessible publicly)
        :param regex: tuple of str, file patterns to upload, e.g., ('*.jpg', '.*png') to upload jpg and png images
        """
        assert isdir(data_dir)
        # load file path
        files = self.load_files(data_dir=data_dir, regex=regex)

        # create bucket on S3
        if bucket_name is None:
            bucket_name = basename(data_dir)
        bucket_name = bucket_name.lower()
        self.create_bucket(bucket_name=bucket_name)

        # start upload workers
        self._stop_event = threading.Event()
        self.start(bucket_name=bucket_name, bucket_folder=bucket_folder, is_public=is_public)

        # set up time estimator
        num_total_files = len(files)
        eta = ETA(num_total=num_total_files)
        cnt = 0
        print('Queue Preparing ...')
        t_start = time()
        f = open('file_list.txt', 'w')
        for file in files:
            key = file.split(data_dir)[-1]
            if key[0] in ['/']:
                key = key[1:]
            self.queue.put([file, key])
            f.writelines(key + '\n')
            cnt += 1
            if cnt % 100 == 0:
                print_str = '[{:0{w}d}/{:0{w}d}] ETA: {}'.format(
                    cnt, num_total_files, eta(t_start=t_start, t_end=time(), n_start=cnt - 100, n_end=cnt),
                    w=len(str(num_total_files))
                )
                print(print_str)
                t_start = time()
        f.close()
        key = 'file_list.txt' if bucket_folder is None else join(bucket_folder, 'file_list.txt')
        self.s3.upload_file('file_list.txt', bucket_name, key, ExtraArgs={'ACL': 'public-read'})
        with open('meta.txt', 'w') as f:
            f.writelines('bucket_name:{}\n'
                         'bucket_folder:{}\n'
                         'local_data_dir:{}\n'.format(bucket_name, bucket_folder, realpath(data_dir)))
        key = 'meta.txt' if bucket_folder is None else join(bucket_folder, 'meta.txt')
        self.s3.upload_file('meta.txt', bucket_name, key, ExtraArgs={'ACL': 'public-read'})

        while not self.queue.empty():
            print('{} files left ...'.format(self.queue.qsize()))
            sleep(1)

        self.stop()
        print('{} files are uploaded to Bucket {}'.format(cnt, bucket_name))
        #print('File names are listed in Bucket {}/file_list.txt'.format(bucket_name))

    def is_running(self):
        return self._stop_event is not None and not self._stop_event.is_set()

    def stop(self, timeout=None):
        if self.is_running():
            self._stop_event.set()

        for thread in self._threads:
            if thread.is_alive():
                thread.join(timeout)
            print('%s-%s: Stop' % (self.my_name, thread.name))
        self._threads = []
        self._stop_event = None
        self.queue = None

    def start(self, **kwargs):
        for idx in range(self.num_workers):
            try:
                thread = threading.Thread(target=self._upload, name=str(idx), kwargs=kwargs, daemon=False)
                self._threads.append(thread)
                thread.start()
                print('%s-%s: Start' % (self.my_name, thread.name))
            except:
                import traceback
                traceback.print_exc()

    def _upload(self, bucket_name, bucket_folder=None, is_public=False):
        """
        upload data to S3
        :param bucket_name: str, name of bucket
        :param bucket_folder: str, folder name in the bucket, where the data is uploaded to
        :return:
        """
        session = boto3.session.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token
        )
        s3 = session.resource('s3')
        while self.is_running():
            try:
                if not self.queue.empty():
                    data_path, key = self.queue.get()
                    data = open(data_path, 'rb')
                    if bucket_folder is not None:
                        key = join(bucket_folder, key)
                    if is_public:
                        s3.meta.client.upload_file(Filename=data_path, Bucket=bucket_name, Key=key,
                                                   ExtraArgs={'ACL': 'public-read'})
                        # s3.Bucket(bucket_name).put_object(Key=key, Body=data, ACL='public-read')
                    else:
                        s3.meta.client.upload_file(Filename=data_path, Bucket=bucket_name, Key=key)
                        # s3.Bucket(bucket_name).put_object(Key=key, Body=data)
                else:
                    sleep(self.wait_time)
            except:
                import traceback
                traceback.print_exc()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser('Upload data to AWS S3')
    parser.add_argument('--bucket', type=str, required=True, help='bucket name')
    parser.add_argument('--folder', type=str, default=None, help='the folder name under the bucket')
    parser.add_argument('--data_dir', type=str, default=None, help='path to the folder of the dataset')
    parser.add_argument('--public', action='store_true', help='make the data public on S3')
    parser.add_argument('--workers', type=int, default=16, help='number of workers for parallel uploading')
    parser.add_argument('--action', choices=['upload', 'delete', 'check'], default='upload', help='')
    args = parser.parse_args()

    bucket_name = args.bucket
    bucket_folder = args.folder
    action = args.action
    data_dir = args.data_dir
    is_public = args.public
    num_workers = args.workers

    uploader = UploadS3Parallel(num_workers=num_workers, max_queue_size=num_workers*16)

    if action == 'upload':
        uploader(data_dir=data_dir, bucket_name=bucket_name, bucket_folder=bucket_folder, is_public=is_public, regex=('*',))
    elif action == 'check':
        uploader.check_files(bucket_name=bucket_name)
    elif action == 'delete':
        uploader.delete_bucket(bucket_name=bucket_name)
    else:
        raise Exception('Cannot recognize the action!')

