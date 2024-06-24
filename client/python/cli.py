import socket
import threading
import time
import argparse

def send_message(sock, ip, port, message):
    message = f"SND {message}"
    sock.sendto(message.encode(), (ip, port))
    return

def send_ack(sock, ip, port, id):
    message = f"ACK {id}"
    sock.sendto(message.encode(), (ip, port))
    return

def request_history(sock, ip, port):
    sock.sendto("HIS".encode(), (ip, port))
    return

def send_udp_packet(sock, ip, port, stop_event):
    while not stop_event.is_set():
        cmd = input("What would you like to do?\nsend a message (1)\nacknowledge a message (2)\nrequest all the unread messages (3)\nexit (4)\nans: ")
        if cmd.lower() == 'exit' or cmd.startswith("4"):
            stop_event.set()
            break
        if cmd.startswith("1"):
            message = input("Send what?: ")
            send_message(sock, ip, port, message)
        elif cmd.startswith("2"):
            message = input("Which id?: ")
            send_ack(sock, ip, port, message)
        elif cmd.startswith("3"):
            request_history(sock, ip, port)
        else:
            print("Unsupported cmd type")
        time.sleep(1)

def receive_udp_packet(sock, stop_event):
    while not stop_event.is_set():
        try:
            sock.settimeout(1)  # Timeout for checking the stop_event
            data, server = sock.recvfrom(4096)
            print(data.decode())
        except socket.timeout:
            continue
        except Exception as e:
            if not stop_event.is_set():
                print(f"Error receiving message: {e}")
            break

def signal_handler(sig, frame, stop_event):
    print("\nCtrl+C detected. Exiting gracefully...")
    stop_event.set()

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Send and receive UDP packets to/from a specified IP address and port.")
    parser.add_argument("ip", type=str, help="The IP address to send the UDP packets to.")
    parser.add_argument("port", type=int, help="The port to send the UDP packets to.")
    
    args = parser.parse_args()

    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Create a stop event
    stop_event = threading.Event()

    # Start the receiving thread
    receive_thread = threading.Thread(target=receive_udp_packet, args=(sock, stop_event))
    receive_thread.start()

    try:
        # Start the sending function
        send_udp_packet(sock, args.ip, args.port, stop_event)
    except KeyboardInterrupt:
        stop_event.set()

    # Wait for the receiving thread to finish
    receive_thread.join()

    # Close the socket after sending is done
    sock.close()
