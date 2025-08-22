import paramiko
import logging

def connect_conversion_host(
    controller_host: str,
    controller_user: str,
    controller_pass: str = None,
    controller_key: str = None,
    conv_host: str = None,
    conv_user: str = None,
    conv_key: str = None
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # SSH to controller
    try:
        ctrl_client = paramiko.SSHClient()
        ctrl_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info(f"Connecting to controller {controller_host} as {controller_user}...")
        connect_kwargs = {
            "hostname": controller_host,
            "username": controller_user,
            "look_for_keys": False,
            "allow_agent": False
        }
        if controller_key:
            connect_kwargs["key_filename"] = controller_key
        elif controller_pass:
            connect_kwargs["password"] = controller_pass
        else:
            raise ValueError("Either controller_pass or controller_key must be provided")
        ctrl_client.connect(**connect_kwargs)
        logger.info("Connected to controller successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to controller: {e}")
        raise

    # Create channel from controller to conversion host
    try:
        transport = ctrl_client.get_transport()
        dest_addr = (conv_host, 22)
        local_addr = ('127.0.0.1', 0)
        logger.info(f"Opening channel from controller to conversion host {conv_host}...")
        channel = transport.open_channel("direct-tcpip", dest_addr, local_addr)
        logger.info("Channel opened successfully.")
    except Exception as e:
        logger.error(f"Failed to open channel to conversion host: {e}")
        ctrl_client.close()
        raise

    # Connect to conversion host through ssh channel
    try:
        conv_client = paramiko.SSHClient()
        conv_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        logger.info(f"Connecting to conversion host {conv_host} as {conv_user}...")
        conv_client.connect(
            conv_host,
            username=conv_user,
            key_filename=conv_key,
            sock=channel
        )
        logger.info("Connected to conversion host successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to conversion host: {e}")
        ctrl_client.close()
        raise

    # Test run command
    try:
        stdin, stdout, stderr = conv_client.exec_command("hostname; whoami")
        output = stdout.read().decode()
        logger.info(f"Conversion host says: {output}")
    except Exception as e:
        logger.error(f"Failed to execute command on conversion host: {e}")
        conv_client.close()
        ctrl_client.close()
        raise

    return conv_client, ctrl_client

def disconnect_conv_host(conv_client, ctrl_client):
    conv_client.close()
    ctrl_client.close()