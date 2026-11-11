import hashlib
import json


def content_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()


class Portal:
    def __init__(self):
        self.drafts = {}
        self.receipts = []
        self.session_valid = True
        self.drop_response = False

    def draft(self, approval, content):
        if not self.session_valid:
            raise PermissionError('会话已过期')
        self.drafts[approval] = dict(content)
        return dict(approval=approval, digest=content_digest(content))

    def submit(self, approval):
        if not self.session_valid:
            raise PermissionError('会话已过期')
        content = self.drafts[approval]
        receipt = dict(number=f'R{len(self.receipts)+1}', approval=approval, digest=content_digest(content))
        self.receipts.append(receipt)
        if self.drop_response:
            raise ConnectionError('提交响应丢失')
        return receipt

    def query(self, approval):
        return [dict(receipt) for receipt in self.receipts if receipt['approval'] == approval]


def collect(payload):
    portal = Portal()
    records = []
    for row in payload['applications']:
        portal.draft(row['approval'], row['content'])
        receipt = portal.submit(row['approval'])
        records.append(dict(key=receipt['number'], **receipt))
    return records
