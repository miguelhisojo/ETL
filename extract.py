import os
import sys
import requests
import logging

module_logger = logging.getLogger('etl_application.extract')


class Extract:
    def __init__(self, url, file_name, bucket_name, path):
        self.url = url
        self.file_name = file_name
        self.bucket_name = bucket_name
        self.path = path
        self.logger = logging.getLogger('etl_application.extract.Extract')
        self.logger.info('creating an instance of Extract')

    def download_url(self: object) -> None:
        """
        Download file from URL, no ssl verification
        :return: None
        """
        self.logger.info(f"\t Downloading file {self.file_name} from {self.url} ")
        zip_file = os.path.join(self.path, self.file_name)

        try:
            if not os.path.exists(self.path):
                os.makedirs(self.path)

            with open(zip_file, 'wb') as f:
                self.logger.info(f"\t - Request {zip_file}")
                response = requests.get(self.url,
                                        verify=False,
                                        timeout=3,
                                        stream=True)
                response.raise_for_status()
                total = response.headers.get('content-length')

                if total is None:
                    f.write(response.content)
                else:
                    if response.status_code == 200:
                        downloaded = 0
                        total = int(total)
                        for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                            downloaded += len(data)
                            f.write(data)
                            done = int(50 * downloaded / total)
                            self.logger.info('\r[{}{}]'.format('█' * done, '.' * (50 - done)))
                            sys.stdout.write('\r[{}{}]'.format('█' * done, '.' * (50 - done)))
                            sys.stdout.flush()
                        print('\n')

                    else:
                        self.logger.info('Web site does not exist')

        except (ValueError, requests.exceptions.Timeout):
            self.logger.exception("Timeout Error:", exc_info=True)
        except (ValueError, requests.exceptions.ConnectionError):
            self.logger.exception("Error Connecting:", exc_info=True)
        except (ValueError, requests.exceptions.HTTPError):
            self.logger.exception("Http Error:", exc_info=True)
        except (ValueError, requests.exceptions.RequestException):
            self.logger.exception("Exception: Other Errors: ", exc_info=True)
