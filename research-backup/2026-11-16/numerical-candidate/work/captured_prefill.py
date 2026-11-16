"""Private historical capture plus conditional arithmetic, never authorization.

Caller reference provenance/policy remain unverified. No public mount or receipt
capability. Complete response observation does not mean remote delivery. Numeric
failure cannot retract downloaded bytes. Do not persist exception objects/locals.
"""
from dataclasses import dataclass
from result_download_api import _observe_selected_download,PAYLOAD_LIMIT
from prefill_comparison import observe_prefill_difference

@dataclass(frozen=True,slots=True)
class _NumericObservation:
    element_count:int
    violation_count:int
    max_abs_error:float
    within_tolerance:bool

@dataclass(frozen=True,slots=True)
class _CapturedDifference:
    association:object
    numeric:_NumericObservation

async def _observe_selected_prefill(*,service,scope,receive,reference,
                                    reference_shape,atol,rtol,
                                    max_payload_bytes=PAYLOAD_LIMIT):
    association=await _observe_selected_download(service=service,scope=scope,
        receive=receive,max_payload_bytes=max_payload_bytes)
    numeric=observe_prefill_difference(association.observation.body,reference,
        reference_shape=reference_shape,atol=atol,rtol=rtol)
    return _CapturedDifference(association,_NumericObservation(**numeric))
