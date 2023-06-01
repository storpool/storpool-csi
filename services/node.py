"""
Bla-bla
"""
import distutils.util
import logging
import os.path
import subprocess

from pathlib import Path

from storpool import spapi, spconfig, sptypes

from grpc_interceptor.exceptions import (
    NotFound,
    Internal,
    AlreadyExists,
    InvalidArgument,
)
from pb import csi_pb2
from pb import csi_pb2_grpc

import utils

RESIZE_TOOL_MAP = {
    "ext4": "/sbin/resize2fs"
}

logger = logging.getLogger("NodeService")


def volume_is_attached(volume_name: str) -> bool:
    """
    Checks whether a StorPool volume is attached to the current node
    """
    return Path("/dev/storpool-byid/" + volume_name).is_block_device()


def volume_get_real_path(volume_name: str) -> str:
    """
    Returns the "/dev/sp-X" device which represents the volume_name
    """
    return str(Path("/dev/storpool-byid/" + volume_name).readlink())


def volume_is_formatted(volume_name: str) -> bool:
    """
    Checks whether a StorPool volume is formatted
    """
    return (
        subprocess.run(
            ["blkid", volume_get_real_path(volume_name)], check=False
        ).returncode
        == 0
    )


def volume_get_fs(volume_name: str) -> str:
    """
    Returns the filesystem of a volume
    """
    return subprocess.run(
        ["blkid", "-o", "value", "-s", "TYPE", volume_get_real_path(volume_name)],
        check=False,
        capture_output=True,
        encoding="utf-8",
    ).stdout.strip()


def volume_is_mounted(volume_name: str) -> bool:
    """
    Checks if a volume is mounted
    """
    system_mounts = utils.get_mounted_devices()
    return (
        len(
            [
                mount
                for mount in system_mounts
                if mount["device"] == volume_get_real_path(volume_name)
            ]
        )
        > 0
    )


def volume_get_mount_info(volume_name: str) -> dict:
    """
    Retrieves information about a mount
    """
    system_mounts = utils.get_mounted_devices()
    return [
        mount
        for mount in system_mounts
        if mount["device"] == volume_get_real_path(volume_name)
    ][0]


def generate_mount_options(readonly: bool, mount_flags) -> str:
    """
    Generates mount options taking into account if the volume is read-only
    """
    mount_options = ["discard"]

    if readonly:
        mount_options.append("ro")
    else:
        mount_options.append("rw")

    mount_options.extend(mount_flags)

    if len(mount_options) == 1:
        return mount_options[0]

    return ",".join(mount_options)


class NodeServicer(csi_pb2_grpc.NodeServicer):
    """
    Provides NodeService implementation
    """

    def __init__(self):
        self._config = spconfig.SPConfig(os.environ.get("SP_NODE_NAME", None))
        self._sp_api = spapi.Api.fromConfig()
        self._node_id = (
            str(self._config["SP_CLUSTER_ID"]).lower()
            + "."
            + str(self._config["SP_OURID"])
        )

    def NodeGetInfo(self, request, context):
        return csi_pb2.NodeGetInfoResponse(
            node_id=self._node_id,
            max_volumes_per_node=sptypes.MAX_CLIENT_DISKS,
        )

    def NodeGetCapabilities(self, request, context):
        response = csi_pb2.NodeGetCapabilitiesResponse()

        stage_unstage_cap = response.capabilities.add()
        stage_unstage_cap.rpc.type = stage_unstage_cap.RPC.STAGE_UNSTAGE_VOLUME

        volume_expand_cap = response.capabilities.add()
        volume_expand_cap.rpc.type = volume_expand_cap.RPC.EXPAND_VOLUME

        return response

    def NodeStageVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume id.")

        if not request.HasField("volume_capability"):
            raise InvalidArgument("Missing volume capabilities.")

        if not request.staging_target_path:
            raise InvalidArgument("Missing staging path.")

        if not volume_is_attached(request.volume_id):
            logger.error(
                "Volume %s is not attached to %s.",
                request.volume_id,
                self._node_id,
            )
            raise NotFound(
                f"""StorPool volume {request.volume_id} is not attached to node {self._node_id}."""
            )

        if request.volume_capability.WhichOneof("access_type") == "mount":
            logger.info(
                "Staging mount volume: %s to path: %s",
                request.volume_id,
                request.staging_target_path,
            )

            volume_requested_fs = "ext4"
            logger.debug("Assuming file system %s", volume_requested_fs)

            if request.volume_capability.mount.fs_type:
                volume_requested_fs = request.volume_capability.mount.fs_type
                logger.debug(
                    "CO specified file system: %s", volume_requested_fs
                )

            logger.debug(
                "CO specified readonly: %r",
                request.publish_context["readonly"],
            )

            if request.volume_capability.mount.mount_flags:
                logger.debug(
                    """CO specified the following mount options: %r""",
                    request.volume_capability.mount.mount_flags,
                )

            mount_options = generate_mount_options(
                bool(
                    distutils.util.strtobool(
                        request.publish_context["readonly"]
                    )
                ),
                request.volume_capability.mount.mount_flags,
            )

            if not volume_is_mounted(request.volume_id):
                if not volume_is_formatted(request.volume_id):
                    logger.debug(
                        """Volume %s is not formatted, formatting with %s""",
                        request.volume_id,
                        volume_requested_fs,
                    )
                    format_command = subprocess.run(
                        [
                            "mkfs." + volume_requested_fs,
                            "/dev/storpool-byid/" + request.volume_id,
                        ],
                        stdout=subprocess.DEVNULL,
                        encoding="utf-8",
                        capture_output=False,
                        check=False,
                    )
                    if format_command.returncode != 0:
                        logger.error(
                            """Failed to format volume %s with the following error: %s""",
                            request.volume_id,
                            format_command.stderr,
                        )
                        raise Internal(
                            f"""StorPool volume {request.volume_id} format
                             failed with error: {format_command.stderr}"""
                        )
                else:
                    volume_current_fs = volume_get_fs(request.volume_id)
                    if volume_requested_fs != volume_current_fs:
                        logger.error(
                            """Volume %s is already formatted with %s""",
                            request.volume_id,
                            volume_current_fs,
                        )
                        raise AlreadyExists(
                            f"""StorPool volume {request.volume_id} is already formatted
                             with {volume_current_fs} but CO tried to
                             stage it with {volume_requested_fs}"""
                        )

                logger.debug(
                    """Volume %s is not mounted, mounting at %s""",
                    request.volume_id,
                    request.staging_target_path,
                )

                mount_command = subprocess.run(
                    [
                        "mount",
                        "-o",
                        mount_options,
                        volume_get_real_path(request.volume_id),
                        request.staging_target_path,
                    ],
                    encoding="utf-8",
                    capture_output=True,
                    check=False,
                )

                if mount_command.returncode != 0:
                    logger.error(
                        """Failed to mount volume %s with the following error: %s""",
                        request.volume_id,
                        mount_command.stderr,
                    )
                    raise Internal(
                        f"""The following error occurred while
                        mounting StorPool volume {request.volume_id}: {mount_command.stderr}"""
                    )
            else:
                volume_mount_info = volume_get_mount_info(request.volume_id)

                if volume_mount_info["target"] != request.staging_target_path:
                    logger.error(
                        """Volume %s is already mounted at %s""",
                        request.volume_id,
                        request.staging_target_path,
                    )
                    raise AlreadyExists(
                        f"""StorPool volume {request.volume_id} is
                         already mounted at {volume_mount_info['target']}"""
                    )

                if (
                    request.volume_capability.mount.mount_flags
                    and volume_mount_info["options"] != mount_options
                ):
                    logger.error(
                        """Volume %s is already mounted with %s""",
                        request.volume_id,
                        volume_mount_info["options"],
                    )
                    raise AlreadyExists(
                        f"""StorPool volume {request.volume_id} is
                         already mounted with {volume_mount_info['options']}"""
                    )

        return csi_pb2.NodeStageVolumeResponse()

    def NodeUnstageVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume id")

        if not request.staging_target_path:
            raise InvalidArgument("Missing stating target path")

        if not volume_is_attached(request.volume_id):
            raise NotFound(
                f"""StorPool volume {request.volume_id} is not attached to node {self._node_id}"""
            )

        logger.info(
            """Unstaging volume %s from path %s""",
            request.volume_id,
            request.staging_target_path,
        )

        if volume_is_mounted(request.volume_id):
            logger.debug("Volume %s is mounted, unmounting", request.volume_id)
            unmount_command = subprocess.run(
                ["umount", request.staging_target_path],
                encoding="utf-8",
                capture_output=True,
                check=False,
            )
            if unmount_command.returncode != 0:
                logger.error(
                    """Failed to unmount volume %s with the following error: %s""",
                    request.volume_id,
                    unmount_command.stderr,
                )
                raise Internal(
                    context.set_details(
                        f"""The following error occurred while unmounting
                         StorPool volume {request.volume_id}: {unmount_command.stderr}"""
                    )
                )

        return csi_pb2.NodeUnstageVolumeRequest()

    def NodePublishVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume id")

        if not request.target_path:
            raise InvalidArgument("Missing target path")

        if not request.HasField("volume_capability"):
            raise InvalidArgument("Missing volume capabilities")

        logger.info(
            "Publishing volume %s at %s",
            request.volume_id,
            request.target_path,
        )

        target_path = Path(request.target_path)

        if not target_path.exists():
            logger.debug(
                "Target path %s doesn't exist, creating it.",
                request.target_path,
            )
            target_path.mkdir(mode=755, parents=True, exist_ok=True)

        if not target_path.is_mount():
            logger.debug(
                "Volume %s is not mounted, mounting it.", request.volume_id
            )
            mount_options = ["bind"]

            if request.readonly:
                mount_options.append("ro")
            else:
                mount_options.append("rw")

            mount_options.extend(request.volume_capability.mount.mount_flags)

            mount_command = subprocess.run(
                [
                    "mount",
                    "-o",
                    ",".join(mount_options),
                    request.staging_target_path,
                    request.target_path,
                ],
                encoding="utf-8",
                capture_output=False,
                check=False,
                stdout=subprocess.DEVNULL,
            )

            if mount_command.returncode != 0:
                logger.error(
                    "Binding volume %s failed with: %s",
                    request.volume_id,
                    mount_command.stderr,
                )
                raise Internal(
                    f"""The following error occurred
                     while binding StorPool volume {request.volume_id}: {mount_command.stderr}"""
                )

        return csi_pb2.NodePublishVolumeResponse()

    def NodeUnpublishVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume id")

        if not request.target_path:
            raise InvalidArgument("Missing target path")

        logger.info(
            """Unpublishing volume %s from %s""",
            request.volume_id,
            request.target_path,
        )

        target_path = Path(request.target_path)

        if target_path.is_mount():
            logger.debug(
                "Volume %s is mounted, unmounting it", request.volume_id
            )
            unmount_command = subprocess.run(
                ["umount", request.target_path],
                encoding="utf-8",
                capture_output=False,
                check=False,
                stdout=subprocess.DEVNULL,
            )

            if unmount_command.returncode != 0:
                logger.error(
                    "Unbinding volume %s failed with: %s",
                    request.volume_id,
                    unmount_command.stderr,
                )
                raise Internal(
                    f"""The following error occurred while unbinding
                     StorPool volume {request.volume_id}: {unmount_command.stderr}"""
                )

        if target_path.is_dir():
            logger.debug(
                "Volume target path %s exists, removing it",
                request.target_path,
            )
            remove_target_path_command = subprocess.run(
                ["rmdir", request.target_path],
                encoding="utf-8",
                capture_output=False,
                check=False,
                stdout=subprocess.DEVNULL,
            )

            if remove_target_path_command.returncode != 0:
                logger.error(
                    """Failed to remove target path %s, error: %s""",
                    request.volume_id,
                    remove_target_path_command.stderr,
                )
                raise Internal(
                    f"""The following error occurred while removing
                     the target path {request.volume_id}: {remove_target_path_command.stderr}"""
                )

        return csi_pb2.NodeUnpublishVolumeResponse()

    def NodeExpandVolume(self, request, context):
        """
        Handles FS resize accordingly
        :param request:
        :param context:
        :return:
        """

        if not request.volume_id:
            raise InvalidArgument("Missing volume id.")

        logger.info(f"Extending volume {request.volume_id} file system")

        volume_fs = volume_get_fs(request.volume_id)

        logger.debug(f"Detected volume {request} file system type: {volume_fs}")

        try:
            extend_fs_tool = RESIZE_TOOL_MAP[volume_fs]

            logger.debug(f"Using {extend_fs_tool} to extend the file system")

            extend_command = subprocess.run([
                extend_fs_tool,
                volume_get_real_path(request.volume_id)
            ],
                encoding="utf-8",
                capture_output=True,
                check=False,
            )

            if extend_command.returncode != 0:
                error_message = f"Extending the file system for volume {request.volume_id} failed with: {extend_command.stderr}"
                logger.error(error_message)
                raise Internal(error_message)

            logger.debug(f"Resize tool output: {extend_command.stdout}")

            expand_volume_response = csi_pb2.NodeExpandVolumeResponse()
            return expand_volume_response
        except KeyError:
            logger.error(f"CO requested to extend an unsupported file system: {volume_fs}")
            raise Internal(f"Unsupported file system: {volume_fs}")
