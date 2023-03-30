"""
This module contains various utility functions
"""


def csi_node_id_to_sp_node_id(csi_node_id: str) -> int:
    """
    Converts CSI node_id to a StorPool node id
    :param csi_node_id: CSI node_id as reported by the NodeService
    :type csi_node_id: str
    :return: StorPool Node id, value used in SP_OURID
    :rtype: int
    """
    return int(csi_node_id.split(".").pop())


def get_mounted_devices() -> list[dict]:
    """
    Returns all mounts currently present on the node
    :return: A list containing dictionaries with information about a mount
    :rtype: list
    """
    result = []
    with open("/proc/mounts") as file:
        mounts = [mount.strip("\n") for mount in file.readlines()]
        for mount in mounts:
            attributes = mount.split(" ")
            result.append(
                {
                    "device": attributes[0],
                    "target": attributes[1],
                    "filesystem": attributes[2],
                    "options": attributes[3],
                }
            )
        return result
