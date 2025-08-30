import socket
import logging
import signal


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._active_connections = set()  

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
        logging.info('action: server_start | result: success')
        
        try:
            while not self._shutdown_requested:
                try:
                    self._server_socket.settimeout(1.0)
                    client_sock = self.__accept_new_connection()
                    self.__handle_client_connection(client_sock)
                except socket.timeout:
                    continue
                except OSError as e:
                    if not self._shutdown_requested:
                        logging.error(f"action: accept_connection | result: fail | error: {e}")
                    break
        finally:
            self._cleanup()

    def _signal_handler(self, signum, frame):
        """
        Handle shutdown signals gracefully
        """
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT" if signum == signal.SIGINT else f"SIGNAL_{signum}"
        logging.info(f'action: signal_received | signal: {signal_name} | active_connections: {len(self._active_connections)} | result: initiating_graceful_shutdown')
        self._shutdown_requested = True

    def _cleanup(self):
        """
        Clean up server resources - close all active connections and server socket
        """
        logging.info('action: server_shutdown | result: in_progress')
        
        if self._active_connections:
            logging.info(f'action: closing_client_connections | count: {len(self._active_connections)}')
            connections_to_close = self._active_connections.copy()
            for client_sock in connections_to_close:
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                    client_sock.close()
                    logging.info('action: close_client_connection | result: success')
                except OSError as e:
                    logging.warning(f'action: close_client_connection | result: fail | error: {e}')
            self._active_connections.clear()
        
        if self._server_socket:
            try:
                self._server_socket.shutdown(socket.SHUT_RDWR)
                self._server_socket.close()
                logging.info('action: close_server_socket | result: success')
            except OSError as e:
                logging.warning(f'action: close_server_socket | result: fail | error: {e}')
        
        logging.info('action: server_shutdown | result: success')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        self._active_connections.add(client_sock)
        try:
            msg = self.__recv_complete_message(client_sock, 1024)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg: {msg}')
            response = "{}\n".format(msg).encode('utf-8')
            self.__send_complete_message(client_sock, response)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            self._active_connections.discard(client_sock)
            try:
                client_sock.close()
                logging.info("action: close_client_connection | result: success")
            except OSError as e:
                logging.warning(f"action: close_client_connection | result: fail | error: {e}")

    def __recv_complete_message(self, client_sock, buffer_size):
        """
        Receive complete message handling short-reads
        
        Continues receiving until a newline is found or connection is closed
        """
        message = b''
        while True:
            chunk = client_sock.recv(buffer_size)
            if not chunk:  
                break
            message += chunk
            if b'\n' in message:  
                break
        return message.rstrip().decode('utf-8')

    def __send_complete_message(self, client_sock, message):
        """
        Send complete message handling short-writes
        
        Ensures all bytes are sent by retrying until complete
        """
        total_sent = 0
        message_length = len(message)
        
        while total_sent < message_length:
            sent = client_sock.send(message[total_sent:])
            if sent == 0:
                raise RuntimeError("Socket connection broken")
            total_sent += sent

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        c.settimeout(None)
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
    