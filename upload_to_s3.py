from os.path import basename, join, isfile, isdir, realpath, expanduser, dirname, exists, splitext
from glob import glob
import threading
from time import sleep, time
from os import walk, makedirs
from multiprocessing import cpu_count
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
    def __init__(self, region='us-west-1', credentials=join(dirname(realpath(__file__)), 'credentials')):
        self.num_workers = min(cpu_count() * 4, 16)
        self.max_queue_size = self.num_workers * 4
        self.queue = queue.Queue(maxsize=self.max_queue_size)
        self._stop_event = None
        self._threads = []
        self.wait_time = .1
        self.my_name = 'Thread'
        self.region = region
        self._credentials_keys = ['aws_access_key_id', 'aws_secret_access_key', 'aws_session_token']
        self._credentials = self.load_credentials(credentials)
        self.s3 = boto3.client(
            's3',
            region_name=self.region,
            aws_access_key_id=self._credentials[self._credentials_keys[0]],
            aws_secret_access_key=self._credentials[self._credentials_keys[1]],
            aws_session_token=self._credentials[self._credentials_keys[2]]
        )

    def load_credentials(self, path_to_credentials):
        if not exists(path_to_credentials) or not isfile(path_to_credentials):
            raise Exception('Cannot find the credential file {}'.format(path_to_credentials))
        credentials = {}
        with open(path_to_credentials, 'r') as f:
            for line in f.readlines():
                if '=' in line and any([_ in line for _ in self._credentials_keys]):
                    segs = line.strip().split('=')
                    credentials[segs[0].strip()] = '='.join(segs[1:]).strip()
        if len(self._credentials_keys) != len(credentials):
            raise Exception('Cannot find {} from {}'.format(
                [_ for _ in self._credentials_keys if _ not in credentials], path_to_credentials))
        return credentials

    def check_files(self, bucket_name, num_print=float('inf'), verb=True):
        file_list = []
        if bucket_name in self.get_bucket_names():
            cnt = 0
            s3 = boto3.resource(
                's3',
                region_name=self.region,
                aws_access_key_id=self._credentials[self._credentials_keys[0]],
                aws_secret_access_key=self._credentials[self._credentials_keys[1]],
                aws_session_token=self._credentials[self._credentials_keys[2]]
            )
            bucket = s3.Bucket(bucket_name)
            for obj in bucket.objects.all():
                file_list.append(obj.key)
                cnt += 1
                if verb:
                    print(file_list[-1])
                if cnt >= num_print:
                    break
            return file_list
        else:
            print('The Bucket {} does not exist!'.format(bucket_name))
            return []

    def get_bucket_names(self):
        return [bucket['Name'] for bucket in self.s3.list_buckets()['Buckets']]

    def create_bucket(self, bucket_name):
        if bucket_name in self.get_bucket_names():
            print('The Bucket {} already exist!'.format(bucket_name))
            if input('Continue to write into Bucket {}?[y/n]'.format(bucket_name)) != 'y':
                exit(0)
        else:
            try:
                self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': self.region})
            except Exception as e:
                print(e)
                print('Please create bucket manually via AWS console.')
                exit(1)

    def delete_bucket(self, bucket_name, file_name=None):
        if bucket_name in self.get_bucket_names():
            s3 = boto3.resource(
                's3',
                region_name=self.region,
                aws_access_key_id=self._credentials[self._credentials_keys[0]],
                aws_secret_access_key=self._credentials[self._credentials_keys[1]],
                aws_session_token=self._credentials[self._credentials_keys[2]]
            )
            bucket = s3.Bucket(bucket_name)
            cnt_remain = 0
            for obj in bucket.objects.all():
                if file_name is None or len(file_name) == 0 or file_name in obj.key or file_name in ['*']:
                    obj.delete()
                    print('Deleted {}'.format(obj.key))
                else:
                    cnt_remain += 1
            if cnt_remain == 0 and file_name is None:
                bucket.delete()
                print('Deleted Bucket {}'.format(bucket_name))
        else:
            print('The Bucket {} does not exist!'.format(bucket_name))

    def download(self, bucket_name, file_key=None, save_dir='./', verb=True):
        if not exists(save_dir) or not isdir(save_dir):
            makedirs(save_dir)
        save_path = join(save_dir, basename(file_key))
        if verb:
            print('Downloading {}/{} to {}'.format(bucket_name, file_key, save_path))
        try:
            self.s3.download_file(bucket_name, file_key, save_path)
        except Exception as e:
            print('Download failed: {}'.format(e))

        # files = self.check_files(bucket_name=bucket_name, num_print=float('inf'), verb=False)
        # print(files)

        # for idx, file in enumerate(files):
        #     save_path = join(save_dir, )
        #     if verb:
        #         print('Downloading {} to {}'.format(file, join()save_dir))
        #     self.s3.download_file(bucket_name, file, basename(file))

    def load_files(self, data_dir, regex=('*.*',)):
        assert isinstance(regex, (list, tuple)) and len(regex) > 0
        files_path = []
        for ex in regex:
            for root, subdirs, files in walk(data_dir):
                files_path.extend([_ for _ in glob(join(root, ex)) if isfile(_)])
        return files_path

    def __call__(self, data_to_upload, bucket_name=None, bucket_folder=None, is_public=False, regex=('*.*',), verb=False):
        """
        upload data to S3
        Args:
            data_to_upload: str, dir to the data that may include subdirs, or path to a file
            bucket_name: str, name of the bucket, must be lower-case
            bucket_folder: str, folder path under the bucket to store data
            is_public: bool, whether make the data to be public (accessible publicly)
            regex: tuple of str, file patterns to upload, e.g., ('*.jpg', '.*png') to upload jpg and png images
            verb: bool, whether print uploading info

        Returns:

        """
        assert isinstance(data_to_upload, str) and exists(data_to_upload)

        # create bucket on S3
        if bucket_name is None:
            bucket_name = splitext(basename(data_to_upload))[0]
        bucket_name = bucket_name.lower()
        self.create_bucket(bucket_name=bucket_name)

        # upload a single file
        if isfile(data_to_upload):
            key = basename(data_to_upload) if bucket_folder is None else join(bucket_folder, basename(data_to_upload))
            print('Uploading {} to Bucket {}/{} {}'.format(
                    data_to_upload, bucket_name, key, '(public)' if is_public else ''))
            self.s3.upload_file(data_to_upload, bucket_name, key, ExtraArgs={'ACL': 'public-read' if is_public else ''})
            cnt = 1

        else:  # upload files in a folder
            files = self.load_files(data_dir=data_to_upload, regex=regex)

            # start upload workers
            self._stop_event = threading.Event()
            self.start(bucket_name=bucket_name, bucket_folder=bucket_folder, is_public=is_public, verb=verb)

            # set up time estimator
            num_total_files = len(files)
            eta = ETA(num_total=num_total_files)
            cnt = 0
            print('Queue Preparing ...')
            t_start = time()
            f = open('file_list.txt', 'w')
            for file in files:
                key = data_to_upload.join(file.split(data_to_upload)[1:])
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
                             'local_data_dir:{}\n'.format(bucket_name, bucket_folder, realpath(data_to_upload)))
            key = 'meta.txt' if bucket_folder is None else join(bucket_folder, 'meta.txt')
            self.s3.upload_file('meta.txt', bucket_name, key, ExtraArgs={'ACL': 'public-read'})

            while not self.queue.empty():
                print('{} files left ...'.format(self.queue.qsize()))
                sleep(1)

            self.stop()
        print('Done: {} file(s) -> Bucket {}'.format(cnt, bucket_name))

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

    def _upload(self, bucket_name, bucket_folder=None, is_public=False, verb=False):
        """
        upload data to S3
        :param bucket_name: str, name of bucket
        :param bucket_folder: str, folder name in the bucket, where the data is uploaded to
        :return:
        """
        session = boto3.session.Session(
            aws_access_key_id=self._credentials[self._credentials_keys[0]],
            aws_secret_access_key=self._credentials[self._credentials_keys[1]],
            aws_session_token=self._credentials[self._credentials_keys[2]]
        )
        s3 = session.resource('s3')
        while self.is_running():
            try:
                if not self.queue.empty():
                    data_path, key = self.queue.get()
                    # data = open(data_path, 'rb')
                    if bucket_folder is not None:
                        key = join(bucket_folder, key)
                    if verb:
                        print('Uploading {} to Bucket {}/{}'.format(data_path, bucket_name, key))
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
    parser.add_argument('--bucket', type=str, default='zzhang', help='bucket name')
    parser.add_argument('--folder', type=str, default=None, help='the folder name under the bucket for uploading')
    parser.add_argument('--data', type=str, default=None,
                        help='if upload, dir or path to the file(s)'
                             'if download, path to file in the bucket, '
                             'e.g., -data Folder/File.txt if download Bucket/Folder/File.txt')
    parser.add_argument('--public', action='store_true', help='make the data public on S3')
    parser.add_argument('--action', choices=['upload', 'download', 'list', 'delete'], default='upload',
                        help='upload - upload file(s)'
                             'download - download file'
                             'list - list files in a bucket'
                             'delete - delete a bucket or files in a bucket')
    parser.add_argument('--v', action='store_true', help='print info')
    parser.add_argument('--delete-files', type=str, default='*', help='file format to delete, e.g., .py')
    parser.add_argument('--download-dir', type=str, default='./', help='dir to save download file')
    args = parser.parse_args()

    bucket_name = args.bucket
    bucket_folder = args.folder
    action = args.action
    data = args.data
    is_public = args.public

    uploader = UploadS3Parallel()

    if action == 'upload':
        assert data
        uploader(data_to_upload=data, bucket_name=bucket_name, bucket_folder=bucket_folder,
                 is_public=is_public, regex=('*',), verb=args.v)
    elif action == 'list':
        uploader.check_files(bucket_name=bucket_name)
    elif action == 'delete':
        uploader.delete_bucket(bucket_name=bucket_name, file_name=args.delete_files)
    elif action == 'download':
        assert data
        uploader.download(bucket_name=bucket_name, file_key=data, save_dir=args.download_dir)
    else:
        raise Exception('Cannot recognize the action!')

