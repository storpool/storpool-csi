"""
Implement the ControllerService of the CSI spec
"""
import logging
import re

from pathlib import Path
from urllib.parse import urlparse

from storpool import spapi, sptypes
from grpc_interceptor.exceptions import (
    NotFound,
    Internal,
    InvalidArgument,
    AlreadyExists,
    FailedPrecondition,
    ResourceExhausted,
    OutOfRange,
)

from pb import csi_pb2
from pb import csi_pb2_grpc

import utils
import constant

logger = logging.getLogger("ControllerService")


class ControllerServicer(csi_pb2_grpc.ControllerServicer):
    """
    Implement the ControllerService as a gRPC Servicer
    """

    def __init__(self, sp_api_endpoint: str, sp_api_token: str):
        if Path("/etc/storpool.conf").exists():
            logger.debug(
                "Found /etc/storpool.conf, loading API endpoint and token from it"
            )
            self._sp_api = spapi.Api.fromConfig()
        else:
            if sp_api_endpoint is None or sp_api_token is None:
                raise RuntimeError(
                    "StorPool API endpoint or authentication token not specified"
                )

            url = urlparse(sp_api_endpoint, "http")
            logger.debug(
                "Connection to StorPool API at %s with token %s",
                sp_api_endpoint,
                sp_api_token,
            )
            self._sp_api = spapi.Api(
                host=url.hostname, port=url.port, auth=sp_api_token
            )

    def ControllerGetCapabilities(self, request, context):
        response = csi_pb2.ControllerGetCapabilitiesResponse()

        create_delete_volume_cap = response.capabilities.add()
        create_delete_volume_cap.rpc.type = (
            create_delete_volume_cap.RPC.CREATE_DELETE_VOLUME
        )

        publish_unpublish_volume_cap = response.capabilities.add()
        publish_unpublish_volume_cap.rpc.type = (
            publish_unpublish_volume_cap.RPC.PUBLISH_UNPUBLISH_VOLUME
        )

        publish_readonly_cap = response.capabilities.add()
        publish_readonly_cap.rpc.type = (
            publish_readonly_cap.RPC.PUBLISH_READONLY
        )

        return response

    def CreateVolume(self, request, context):
        if not request.name:
            raise InvalidArgument("Missing volume name")

        if not request.volume_capabilities:
            raise InvalidArgument("Missing volume capabilities")

        if request.parameters["template"] is None:
            raise InvalidArgument("Missing volume template name")

        volume_size = self._determine_volume_size(request.capacity_range)

        logger.info(
            f"Provisioning volume {request.name} (template: {request.parameters['template']}, size: {volume_size})",
        )

        try:
            volume_create_result = self._sp_api.volumeCreate(
                {
                    "template": request.parameters["template"],
                    "size": volume_size,
                    "tags": {"csi_name": request.name},
                }
            )

            return self._construct_volume_create_response(
                self._sp_api.volumeInfo(volume_create_result.globalId)
            )
        except spapi.ApiError as error:
            logger.error(f"StorPool API error {error.name}: {error.desc}")
            if error.name == "insufficientResources":
                raise OutOfRange(error.desc)
            elif error.name == "objectDoesNotExist":
                raise InvalidArgument(error.desc)
            else:
                raise Internal(error.desc)

    def DeleteVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume name")

        for attachment in self._sp_api.attachmentsList():
            if attachment.volume == request.volume_id:
                raise FailedPrecondition(
                    f"""StorPool volume {request.volume_id} is still
                    attached to {attachment.client}"""
                )

        logger.info("Deleting volume %s", request.volume_id)

        if request.volume_id in [
            volume.name for volume in self._sp_api.volumesList()
        ]:
            self._sp_api.volumeDelete(request.volume_id)
            logger.debug("Successfully deleted volume %s", request.volume_id)
        else:
            logger.error(
                "Tried to delete volume %s that does not exist",
                request.volume_id,
            )

        return csi_pb2.DeleteVolumeResponse()

    def ValidateVolumeCapabilities(self, request, context):
        response = csi_pb2.ValidateVolumeCapabilitiesResponse()

        if not request.volume_id:
            raise InvalidArgument("Missing volume Id")

        if not request.volume_capabilities:
            raise InvalidArgument("Missing volume capabilities")

        if request.volume_id not in [
            volume.name for volume in self._sp_api.volumesList()
        ]:
            logger.error(
                """Cannot validate volume %s because it doesn't exist.""",
                request.volume_id,
            )
            raise NotFound(
                f"StorPool volume {request.volume_id} does not exist."
            )

        if hasattr(request.parameters, "template"):
            response.confirmed.parameters["template"] = request.parameters[
                "template"
            ]

        for requested_capability in request.volume_capabilities:
            confirmed_capability = csi_pb2.VolumeCapability()
            if requested_capability.WhichOneof("access_type") == "mount":
                logger.debug("Volume %s is of type mount.", request.volume_id)
                confirmed_capability.mount.SetInParent()
                if (
                    requested_capability.access_mode.mode
                    == confirmed_capability.AccessMode.SINGLE_NODE_WRITER
                    or requested_capability.access_mode.mode
                    == confirmed_capability.AccessMode.SINGLE_NODE_READER_ONLY
                ):
                    confirmed_capability.access_mode.mode = (
                        requested_capability.access_mode.mode
                    )
                    response.confirmed.volume_capabilities.append(
                        confirmed_capability
                    )

        return response

    def ControllerPublishVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume Id")

        if not request.node_id:
            raise InvalidArgument("Missing node id")

        if not request.HasField("volume_capability"):
            raise InvalidArgument("Missing volume capabilities")

        logger.info(
            """Publishing volume %s to %s as readonly: %r""",
            request.volume_id,
            request.node_id,
            request.readonly,
        )

        if request.volume_id not in [
            volume.name for volume in self._sp_api.volumesList()
        ]:
            logger.error(
                "Tried publishing volume %s but it doesn't exist.",
                request.volume_id,
            )
            raise NotFound(f"StorPool volume {request.volume_id} not found.")

        if not re.match(constant.CSI_NODE_ID_REGEX, request.node_id):
            logger.error(
                "Tried publishing to invalid node id: %s", request.node_id
            )
            raise NotFound(
                f"Node {request.node_id} is not a StorPool CSI node"
            )

        sp_node_id = utils.csi_node_id_to_sp_node_id(request.node_id)

        if sp_node_id not in self._sp_api.servicesList().clients.keys():
            logger.error(
                "Tried publishing to node %s but it doesn't have a block service running.",
                request.node_id,
            )
            raise NotFound(
                f"StorPool node {request.node_id} doesn't have a block service running."
            )

        if request.volume_id in [
            attachment.volume
            for attachment in self._sp_api.attachmentsList()
            if attachment.client != sp_node_id
        ]:
            logger.error(
                "Volume %s is already attached to another node.",
                request.volume_id,
            )
            raise FailedPrecondition(
                f"""StorPool volume {request.volume_id} is already attach to another node."""
            )

        node_attachments = [
            attachment
            for attachment in self._sp_api.attachmentsList()
            if attachment.client == sp_node_id
        ]

        if request.volume_id in [
            attachment.volume for attachment in node_attachments
        ]:
            attachment = self._get_attachment_for_volume(request.volume_id)
            if (attachment.rights == "rw" and request.readonly) or (
                attachment.rights == "ro" and not request.readonly
            ):
                logger.error(
                    """Tried to attach volume %s to node %s as readonly: %r but it's
                     already attached as %s""",
                    request.volume_id,
                    request.node_id,
                    request.readonly,
                    attachment.rights,
                )
                raise AlreadyExists(
                    f"""StorPool volume {request.volume_id} attached as {attachment.rights}
                     but published as readonly: {request.readonly}"""
                )

        else:
            if len(node_attachments) == sptypes.MAX_CLIENT_DISKS:
                logger.error(
                    "No more volumes can be attached to node %s",
                    request.node_id,
                )
                raise ResourceExhausted(
                    f"No more volumes can be attached to node {request.node_id}"
                )

            volume_reassign = {"volume": request.volume_id}

            if request.readonly:
                volume_reassign["ro"] = [sp_node_id]
            else:
                volume_reassign["rw"] = [sp_node_id]

            logger.debug(
                "Trying to attach volume %s to node %s as readonly: %r",
                request.volume_id,
                request.node_id,
                request.readonly,
            )
            self._sp_api.volumesReassignWait({"reassign": [volume_reassign]})

        return csi_pb2.ControllerPublishVolumeResponse(
            publish_context={"readonly": str(request.readonly)}
        )

    def ControllerUnpublishVolume(self, request, context):
        if not request.volume_id:
            raise InvalidArgument("Missing volume Id")

        logger.info("Unpublishing volume %s", request.volume_id)

        if request.volume_id in [
            attachment.volume for attachment in self._sp_api.attachmentsList()
        ]:
            volume_reassign = {"volume": request.volume_id}

            if request.node_id:
                volume_reassign["detach"] = [
                    utils.csi_node_id_to_sp_node_id(request.node_id)
                ]
                logger.debug(
                    "Detaching volume %s from node %s",
                    request.volume_id,
                    request.node_id,
                )
            else:
                volume_reassign["detach"] = "all"
                logger.debug(
                    "Detaching volume %s from all nodes", request.volume_id
                )

            self._sp_api.volumesReassignWait({"reassign": [volume_reassign]})

        else:
            logger.error("Volume %s isn't attached", request.volume_id)

        return csi_pb2.ControllerUnpublishVolumeResponse()

    def _get_attachment_for_volume(self, volume_name):
        return next(
            attachment
            for attachment in self._sp_api.attachmentsList()
            if attachment.volume == volume_name
        )

    @staticmethod
    def _construct_volume_create_response(volume_info):
        response = csi_pb2.CreateVolumeResponse()

        if volume_info is not None:
            response.volume.volume_id = volume_info.globalId
            response.volume.capacity_bytes = volume_info.size

        return response

    @staticmethod
    def _determine_volume_size(capacity_range):
        if capacity_range.required_bytes > 0 and capacity_range.limit_bytes > 0:
            return max(capacity_range.required_bytes, capacity_range.limit_bytes)
        else:
            return constant.DEFAULT_VOLUME_SIZE

