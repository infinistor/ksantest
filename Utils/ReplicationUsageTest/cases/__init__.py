"""등록된 테스트 케이스. 새 케이스는 cases/tcNN_*.py 로 만들고 아래 목록에 추가한다."""
from cases.tc01_basic import Tc01Basic
from cases.tc02_versions import Tc02Versions
from cases.tc03_prefix import Tc03Prefix
from cases.tc04_tag import Tc04Tag
from cases.tc05_prefix_tag import Tc05PrefixTag
from cases.tc06_delete_marker_off import Tc06DeleteMarkerOff
from cases.tc07_delete_marker_on import Tc07DeleteMarkerOn
from cases.tc08_version_delete import Tc08VersionDelete
from cases.tc09_copy import Tc09Copy
from cases.tc10_multipart import Tc10Multipart
from cases.tc11_checksum import Tc11Checksum
from cases.tc12_tagging import Tc12Tagging
from cases.tc13_sse_s3 import Tc13SseS3, Tc15SseSourceOnly, Tc16SseTargetOnly
from cases.tc14_prefix_delete_marker import Tc14PrefixDeleteMarker

CASES = [
    Tc01Basic, Tc02Versions, Tc03Prefix, Tc04Tag, Tc05PrefixTag, Tc06DeleteMarkerOff, Tc07DeleteMarkerOn,
    Tc08VersionDelete, Tc09Copy, Tc10Multipart, Tc11Checksum, Tc12Tagging, Tc13SseS3, Tc14PrefixDeleteMarker,
    Tc15SseSourceOnly, Tc16SseTargetOnly,
]
