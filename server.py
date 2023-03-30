#!/usr/bin/python3

"""
Main entrypoint of the driver, starts the gRPC server
"""

import argparse
import logging
import os
from concurrent import futures

import grpc
from grpc_interceptor import ExceptionToStatusInterceptor
from pb import csi_pb2_grpc

import services


def getargs() -> argparse.Namespace:
    """Return ArgumentParser instance object"""
    parser = argparse.ArgumentParser(
        description="""StorPool CSI driver""",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--csi-endpoint", type=str, default="unix:///run/csi/sock"
    )

    parser.add_argument(
        "--sp-api-endpoint",
        type=str,
        default=None,
        help="StorPool API endpoint",
    )

    parser.add_argument(
        "--sp-api-token",
        type=str,
        default=None,
        help="StorPool API authentication token",
    )

    parser.add_argument("--log", type=str, default="WARNING", help="Log level")

    parser.add_argument(
        "--worker-threads",
        type=int,
        default=10,
        help="Worker thread count for the gRPC server",
    )

    return parser.parse_args()


def main() -> None:
    """
    Main function running the gRPC server
    :return: None
    """
    args = getargs()

    log_level = getattr(logging, args.log.upper(), None)

    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(funcName)s %(levelname)s: %(message)s",
        level=log_level,
    )

    interceptors = [
        ExceptionToStatusInterceptor(
            status_on_unknown_exception=grpc.StatusCode.INTERNAL
        )
    ]

    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=args.worker_threads),
        interceptors=interceptors,
    )

    identity_servicer = services.IdentityServicer()
    identity_servicer.set_ready(True)

    csi_pb2_grpc.add_IdentityServicer_to_server(identity_servicer, grpc_server)
    csi_pb2_grpc.add_ControllerServicer_to_server(
        services.ControllerServicer(
            sp_api_endpoint=os.environ.get(
                "SP_API_ENDPOINT", args.sp_api_endpoint
            ),
            sp_api_token=os.environ.get("SP_API_TOKEN", args.sp_api_token),
        ),
        grpc_server,
    )
    csi_pb2_grpc.add_NodeServicer_to_server(
        services.NodeServicer(), grpc_server
    )

    grpc_server.add_insecure_port(
        os.environ.get("CSI_ENDPOINT", args.csi_endpoint)
    )
    grpc_server.start()
    grpc_server.wait_for_termination()


if __name__ == "__main__":
    main()
