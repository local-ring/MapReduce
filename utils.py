def zmq_addr(port, transport='tcp', interface='*', multicast=None):
    """
    Generates a ZeroMQ address string based on the given parameters.
    :param port: The port number for the socket connection.
    :param transport: The transport protocol (e.g., 'tcp', 'ipc', 'inproc').
    :param interface: The network interface to bind to (e.g., 'localhost', '*').
                    This is typically used with the 'tcp' transport.
    :param multicast: The multicast group address, used with 'epgm' or 'pgm' transport.
    :return: A ZeroMQ address string.
    """
    if multicast:
        # For multicast, include the interface and multicast group in the address
        return f"{transport}://{interface};{multicast}:{port}"
    else:
        # For other transports, format the address normally
        return f"{transport}://{interface}:{port}"