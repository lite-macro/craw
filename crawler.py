import requests

class plainText():
    def __init__(self, url, payload):
        self.url = url
        self.payload = payload

    def get(self):
        source_code = requests.get(self.url, params=self.payload)
        source_code.encoding = 'utf-8'
        plain_text = source_code.text
        return plain_text

    def post(self):
        source_code = requests.post(self.url, params=self.payload)
        source_code.encoding = 'utf-8'
        plain_text = source_code.text
        return plain_text