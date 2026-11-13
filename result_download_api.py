"""Opt-in owner-only attachment adapter; no default mount, listener or identity."""
import base64
import json
import re
import task_input_api as protocol
import noncompute_application_service as application
import result_artifacts as artifacts

PAYLOAD_LIMIT=4*1024*1024
RESPONSE_LIMIT=8*1024*1024

class _Download:
    def __init__(self,service,max_payload_bytes):
        if service is not None and type(service) is not application.ApplicationService:raise ValueError('invalid service')
        if type(max_payload_bytes) is not int or not 0<max_payload_bytes<=PAYLOAD_LIMIT:raise ValueError('invalid limit')
        self.service=service;self.limit=max_payload_bytes

    async def __call__(self,scope,receive,send):
        if scope.get('type')!='http':raise RuntimeError('HTTP only')
        status=404;body=b'{"error":"result unavailable"}'
        try:
            headers=protocol._headers(scope)
            protocol._need(not ((protocol._FORBIDDEN|{b'cookie',b'origin',b'range',b'transfer-encoding'}) & headers.keys()))
            protocol._need(not scope.get('query_string',b''))
            protocol._need(headers.get(b'content-length',b'0')==b'0')
            match=re.fullmatch(r'/tasks/([0-9a-f]{32})/result',scope.get('path',''))
            protocol._need(match is not None and scope.get('method')=='GET')
            value=headers.get(b'authorization',b'')
            protocol._need(value.startswith(b'Bearer ') and 7<len(value)<=4103)
            credential=value[7:].decode('ascii')
            event=await receive()
            protocol._need(event.get('type')=='http.request' and event.get('body',b'')==b'' and not event.get('more_body',False))
            protocol._need(self.service is not None)
            owner=self.service._download_owner(credential)
            blocks={};total=0;descriptors=[]
            def collect(tile,block):
                nonlocal total
                if type(block) is not bytes or total+len(block)>self.limit:raise artifacts.DownloadRefused()
                total+=len(block);blocks.setdefault(tile,bytearray()).extend(block)
            digest=await self.service.download_result(credential,match.group(1),collect,max_bytes=self.limit,metadata=descriptors.append)
            protocol._need(len(descriptors)==1)
            result=descriptors[0]
            protocol._need(total==result['total_bytes'] and len(blocks)==len(result['tiles']))
            for tile in result['tiles']:
                data=blocks[tile['tile']];protocol._need(len(data)==tile['bytes'])
                tile['data_base64']=base64.b64encode(data).decode('ascii')
            result.update(package='opencompute-result-download-v1',manifest_digest=digest,computation_verified=False,execution_allowed=False)
            encoded=json.dumps(result,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')
            protocol._need(len(encoded)<=RESPONSE_LIMIT)
            # No await between this recheck and invoking first send. Already sent
            # bytes cannot be retracted; this is not continuous authorization.
            protocol._need(self.service._download_owner(credential)==owner)
            body=encoded;status=200
        except Exception:pass
        headers=[(b'content-type',b'application/json'),(b'content-length',str(len(body)).encode()),(b'cache-control',b'no-store'),(b'x-content-type-options',b'nosniff')]
        if status==200:headers.append((b'content-disposition',b'attachment; filename="result.json"'))
        await send({'type':'http.response.start','status':status,'headers':headers})
        await send({'type':'http.response.body','body':body})

def create_app(*,service=None,max_payload_bytes=PAYLOAD_LIMIT):
    """Trusted bootstrap only. Caller owns service lifetime and request deadlines."""
    return _Download(service,max_payload_bytes)
