from utils.conv_host_utils import connect_conversion_host, disconnect_conv_host

def transfer_volume(src_conv_host_client, src_conv_host, dst_conv_host_client, volume_map: dict, nbd_port):
    print("Close nbd port if not closed yet on src conversion host...")
    src_conv_host_client.exec_command("sudo fuser -k {nbd_port}/tcp")
    
    print("Load kernel module nbd on dst conversion host...")
    dst_conv_host_client.exec_command("sudo modprobe nbd max_part=8")

    print("Starting volumes transfer...")

    for key, value in volume_map.items():
        try:
            print(f"Transferring volume {key} on source to {value} on destination...")

            print("Match fs type between src and dst volume...")

            # Get src volume fs type
            stdin, stdout, stderr = src_conv_host_client.exec_command(f"sudo blkid -o value -s TYPE {key}")
            src_fs_type = stdout.read().decode().strip()

            # Get dest volume fs type
            stdin, stdout, stderr = dst_conv_host_client.exec_command(f"sudo blkid -o value -s TYPE {value}")
            dst_fs_type = stdout.read().decode().strip()

            # Return transfer unknown fs type volume
            if not src_fs_type:
                transfer_unknown_fs_type_volume(src_conv_host_client, src_conv_host, dst_conv_host_client, key, value, nbd_port)
                continue

            # Convert to same fs type if not matched
            if src_fs_type and src_fs_type != dst_fs_type:
                dst_conv_host_client.exec_command(f"sudo mkfs.{src_fs_type} {value}")

            src_commands = [
                f"sudo qemu-nbd --format=raw --export-name=disk --port={nbd_port} --persistent --fork --share=1 {key}",
                f"sudo fuser -k {nbd_port}/tcp"
            ]

            dst_commands = [
                f"sudo qemu-nbd --format=raw --connect=/dev/nbd0 --persistent --fork nbd://{src_conv_host}:{nbd_port}/disk",
                f"sudo fsck -y /dev/nbd0",
                f"sudo partclone.{src_fs_type} -c -s /dev/nbd0 -o - | sudo partclone.{src_fs_type} -r -s - -o {value}",
                f"sudo partclone.{src_fs_type} -q -c -s /dev/nbd0 -o - | sudo sha256sum | awk '{{print $1}}' | sudo tee /tmp/src_volume_hash.sha256",
                f"sudo partclone.{src_fs_type} -q -c -s {value} -o - | sudo sha256sum | awk '{{print $1}}' | sudo tee /tmp/dst_volume_hash.sha256",
                f"cmp -s /tmp/src_volume_hash.sha256 /tmp/dst_volume_hash.sha256",
                f"sudo qemu-nbd --disconnect /dev/nbd0",
            ]

            print("Export volume as nbd...")
            stdin, stdout, stderr = src_conv_host_client.exec_command(src_commands[0])
            print(stdout.read().decode())
            print(stderr.read().decode())

            print("Run dst_commands...")
            i = 0
            error_count = 0
            while i < len(dst_commands) and error_count < 5:
                if i == 2 and src_fs_type == "dd":
                    command = f"sudo partclone.dd -d -s /dev/nbd0 -o - | sudo partclone.dd -d -s - -o {value}"
                else:
                    command = dst_commands[i]

                stdin, stdout, stderr = dst_conv_host_client.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                print(stdout.read().decode())
                print(stderr.read().decode())

                if "cmp" in command:
                    if exit_status != 0:
                        print("Volume checksum mismatch, copying volume data again...")
                        error_count += 1
                        i = 1
                        if error_count == 5:
                            print("Max re-copy data retries reached, aborting...")
                            break
                        continue
                    else:
                        print(f"Volume checksum matched, finish transferring volume {key} ...")
                i += 1

            print("Close nbd port...")
            stdin, stdout, stderr = src_conv_host_client.exec_command(src_commands[1])
            print(stdout.read().decode())
            print(stderr.read().decode())

        except Exception as e:
            print(f"Error transferring volume {key}: {e}")

def transfer_unknown_fs_type_volume(src_conv_host_client, src_conv_host, dst_conv_host_client, src_volume, dst_volume, nbd_port):
    src_commands = [
        f"sudo qemu-nbd --format=raw --export-name=disk --port={nbd_port} --persistent --fork --share=1 {src_volume}",
        f"sudo fuser -k {nbd_port}/tcp"
    ]

    dst_commands = [
        f"sudo qemu-nbd --format=raw --connect=/dev/nbd0 --persistent --fork nbd://{src_conv_host}:{nbd_port}/disk",
        f"sudo fsck -y /dev/nbd0",
        f"sudo partclone.dd -d -s /dev/nbd0 -o - | sudo partclone.dd -d -s - -o {dst_volume}",
        f"sudo partclone.dd -q -d -s /dev/nbd0 -o - | sudo sha256sum | awk '{{print $1}}' | sudo tee /tmp/src_volume_hash.sha256",
        f"sudo partclone.dd -q -d -s {dst_volume} -o - | sudo sha256sum | awk '{{print $1}}' | sudo tee /tmp/dst_volume_hash.sha256",
        f"cmp -s /tmp/src_volume_hash.sha256 /tmp/dst_volume_hash.sha256",
        f"sudo qemu-nbd --disconnect /dev/nbd0",
    ]

    print("Export volume as nbd...")
    stdin, stdout, stderr = src_conv_host_client.exec_command(src_commands[0])
    print(stdout.read().decode())
    print(stderr.read().decode())

    print("Run dst_commands...")
    i = 0
    error_count = 0
    while i < len(dst_commands) and error_count < 5:  
        command = dst_commands[i]
        stdin, stdout, stderr = dst_conv_host_client.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        print(stdout.read().decode())
        print(stderr.read().decode())

        if "cmp" in command:
            if exit_status != 0:
                print("Volume checksum mismatch, copying volume data again...")
                error_count += 1
                i = 1
                if error_count == 5:
                    print("Max re-copy data retries reached, aborting...")
                    break
                continue
            else:
                print(f"Volume checksum matched, finish transferring volume {src_volume} ...")
        i += 1

    print("Close nbd port...")
    stdin, stdout, stderr = src_conv_host_client.exec_command(src_commands[1])
    print(stdout.read().decode())
    print(stderr.read().decode())


# Test case conducted on GCP
"""
def main():
    src_ctrl_host = "34.60.179.146"
    src_ctrl_user = "nguyentankdb17"
    src_ctrl_key = "utils/id_rsa"
    src_conv_host = "10.128.0.4"
    src_conv_user = "nguyentankdb17"
    src_conv_key = "utils/id_rsa"

    dst_ctrl_host = "34.122.137.20"
    dst_ctrl_user = "nguyentankdb17"
    dst_ctrl_key = "utils/id_rsa"
    dst_conv_host = "10.128.0.6"
    dst_conv_user = "nguyentankdb17"
    dst_conv_key = "utils/id_rsa"

    volume_map = {
        "/dev/sdb": "/dev/sdb",
        "/dev/sdc": "/dev/sdc"
    }

    nbd_port = 10809

    # src_ctrl_pass = input("Enter source cluster controller host password: ")
    src_conv_host_client, src_ctrl_client = connect_conversion_host(src_ctrl_host, src_ctrl_user, None, src_ctrl_key, src_conv_host, src_conv_user, src_conv_key)

    # dst_ctrl_pass = input("Enter destination cluster controller host password: ")
    dst_conv_host_client, dst_ctrl_client = connect_conversion_host(dst_ctrl_host, dst_ctrl_user, None, dst_ctrl_key, dst_conv_host, dst_conv_user, dst_conv_key)

    transfer_volume(src_conv_host_client, src_conv_host, dst_conv_host_client, volume_map, nbd_port)

    disconnect_conv_host(src_conv_host_client, src_ctrl_client)
    disconnect_conv_host(dst_conv_host_client, dst_ctrl_client)

if __name__ == "__main__":
    main()
"""