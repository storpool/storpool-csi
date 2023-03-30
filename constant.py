"""
Constant values
"""

from storpool import sptypes

CSI_PLUGIN_NAME = "csi.storpool.com"
CSI_PLUGIN_VERSION = "0.0.1"
CSI_NODE_ID_REGEX = sptypes.CLUSTER_NAME_REGEX[0:-1] + r"\.[0-9]{1,2}"
DEFAULT_VOLUME_SIZE = 1073741824
