import socket
import json

class SocketClient:
    """
    A helper class to manage socket connections and communication.
    """
    def __init__(self, host='127.0.0.1', port=65432):
        """
        Initialize the SocketHelper with the host and port.

        Args:
            host (str): The server's hostname or IP address.
            port (int): The port number to connect to.
        """
        self.host = host
        self.port = port

    def send_command(self, command):
        """
        Send a command to the server and receive the response.

        Args:
            command (dict): The command to send, in dictionary format.

        Returns:
            dict: The server's response, parsed as JSON.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            try:
                client_socket.connect((self.host, self.port))
                client_socket.sendall(json.dumps(command).encode('utf-8'))

                # Receive the response in chunks
                response = b""
                while True:
                    chunk = client_socket.recv(4096)
                    if not chunk:
                        break
                    response += chunk

                return json.loads(response.decode('utf-8'))
            except json.JSONDecodeError as e:
                print(f"JSON decoding error: {e}")
                print(f"Raw response: {response.decode('utf-8')}")
                return {"status": "error", "message": "Invalid JSON response"}
            except Exception as e:
                print(f"Socket error: {e}")
                return {"status": "error", "message": str(e)}