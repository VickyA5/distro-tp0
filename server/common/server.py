import socket
import logging
import signal
import threading
from common.protocol import Protocol, ProtocolError
from common.utils import store_bets, load_bets, has_won

store_lock = threading.Lock()

class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False

        self._active_connections = set()
        self._threads = set()
        
        self._agencies_finished = set()  
        self._agencies_that_sent_bets = set()  
        self._total_agencies = 5  
        self._lottery_completed = False  
        self._pending_winners_queries = [] 

        self._connections_lock = threading.Lock()
        self._threads_lock = threading.Lock()
        self._state_lock = threading.Lock()

    def run(self):
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        logging.info('action: server_start | result: success')

        try:
            while not self._shutdown_requested:
                try:
                    self._server_socket.settimeout(1.0)
                    client_sock = self.__accept_new_connection()
                    t = threading.Thread(
                        target=self.__handle_client_connection,
                        args=(client_sock,),
                        daemon=False   
                    )
                    with self._threads_lock:
                        self._threads.add(t)
                    t.start()
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
        logging.info(f'action: signal_received | result: success | signal: {signal_name} | active_connections: {len(self._active_connections)} | shutdown: graceful')
        self._shutdown_requested = True

    def _cleanup(self):
        logging.info('action: server_shutdown | result: in_progress')

        with self._connections_lock:
            for client_sock in list(self._active_connections):
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    client_sock.close()
                    logging.info('action: close_client_connection | result: success')
                except OSError as e:
                    logging.warning(f'action: close_client_connection | result: fail | error: {e}')
            self._active_connections.clear()

        if self._server_socket:
            try:
                self._server_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self._server_socket.close()
                logging.info('action: close_server_socket | result: success')
            except OSError as e:
                logging.warning(f'action: close_server_socket | result: fail | error: {e}')

        with self._threads_lock:
            threads_to_join = list(self._threads)
        for t in threads_to_join:
            t.join(timeout=2.0)  # timeout para no colgar el shutdown

        logging.info('action: server_shutdown | result: success')

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        with self._connections_lock:
            self._active_connections.add(client_sock)

        keep_open = False
        try:
            msg = self.__recv_complete_message(client_sock, 1024)
            addr = client_sock.getpeername()
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg_size: {len(msg)} bytes | msg_type: {msg.split("#")[0] if "#" in msg else msg.split()[0] if msg else "empty"}')

            try:
                if msg.startswith("BATCH#"):
                    self.__handle_batch_message(msg, client_sock)
                elif msg.startswith("BET#"):
                    self.__handle_bet_message(msg, client_sock)
                elif msg.startswith("FINISH_BETS#"):
                    self.__handle_finish_bets_message(msg, client_sock)
                elif msg.startswith("QUERY_WINNERS#"):
                    keep_open = self.__handle_query_winners_message(msg, client_sock)
                    if keep_open:
                        return
                else:
                    raise ProtocolError("unknown_message_type")
                    
            except ProtocolError as e:
                self.__handle_protocol_error(msg, e, client_sock)

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            self.__handle_connection_cleanup(client_sock, keep_open)

    def __recv_complete_message(self, client_sock, buffer_size):
        """
        Receive complete message handling short-reads
        
        For BATCH messages, continues receiving until all expected BET lines are received
        For other messages (BET, FINISH_BETS, QUERY_WINNERS), continues until newline is found
        """
        message = b''
        expected_bets = None
        received_bets = 0
        
        while True:
            chunk = client_sock.recv(buffer_size)
            if not chunk:  
                break
            message += chunk
            
            try:
                decoded = message.decode('utf-8')
                lines = decoded.split('\n')
                
                if decoded.startswith('BATCH#') and expected_bets is None:
                    try:
                        header_parts = lines[0].split('#')
                        if len(header_parts) >= 2:
                            expected_bets = int(header_parts[1])
                    except (ValueError, IndexError):
                        if b'\n' in message:
                            break
                        continue
                
                if expected_bets is not None:
                    received_bets = sum(1 for line in lines[1:] if line.strip().startswith('BET#'))
                    
                    if received_bets >= expected_bets:
                        break
                else:
                    if b'\n' in message:
                        break
                        
            except UnicodeDecodeError:
                continue
        
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
    
    def _get_winners_for_agency(self, agency: str) -> list[str]:
        """
        Get the list of winner documents for a specific agency.
        
        Args:
            agency (str): Agency ID to get winners for
            
        Returns:
            list[str]: List of winner document numbers for the agency
        """
        winners = []
        try:
            all_bets = load_bets()
            for bet in all_bets:
                if str(bet.agency) == agency and has_won(bet):
                    winners.append(bet.document)
        except Exception as e:
            logging.error(f'action: get_winners | result: fail | agency: {agency} | error: {e}')
        
        return winners
    
    def __handle_batch_message(self, msg: str, client_sock):
        """
        Handle BATCH# message type.
        
        Args:
            msg (str): The BATCH message to process
            client_sock: The client socket to send response to
        """
        bets = Protocol.parse_batch(msg)
        with store_lock:
            store_bets(bets)
        with self._state_lock:
            for bet in bets:
                self._agencies_that_sent_bets.add(str(bet.agency))
        logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
        self.__send_complete_message(client_sock, b"OK\n")
    
    def __handle_bet_message(self, msg: str, client_sock):
        """
        Handle BET# message type.
        
        Args:
            msg (str): The BET message to process
            client_sock: The client socket to send response to
        """
        bet = Protocol.parse_bet(msg)
        with store_lock:
            store_bets([bet])
        with self._state_lock:
            self._agencies_that_sent_bets.add(str(bet.agency))
        logging.info(f'action: apuesta_recibida | result: success | cantidad: 1')
        self.__send_complete_message(client_sock, b"OK\n")
    
    def __handle_finish_bets_message(self, msg: str, client_sock):
        """
        Handle FINISH_BETS# message type.
        
        Args:
            msg (str): The FINISH_BETS message to process
            client_sock: The client socket to send response to
        """
        agency = Protocol.parse_finish_bets(msg)
        with self._state_lock:
            self._agencies_finished.add(agency)
            logging.info(
                f'action: finish_bets_received | result: success | agency: {agency} '
                f'| agencies_finished: {len(self._agencies_finished)}/{len(self._agencies_that_sent_bets)} '
                f'| agencies_with_bets: {self._agencies_that_sent_bets}'
            )
            if (len(self._agencies_finished) == len(self._agencies_that_sent_bets)
                and not self._lottery_completed
                and len(self._agencies_that_sent_bets) > 0):
                self._lottery_completed = True
                logging.info('action: sorteo | result: success')
                self._process_pending_winner_queries()
        self.__send_complete_message(client_sock, b"OK\n")
    
    def __handle_query_winners_message(self, msg: str, client_sock) -> bool:
        """
        Handle QUERY_WINNERS# message type.
        
        Args:
            msg (str): The QUERY_WINNERS message to process
            client_sock: The client socket to send response to
            
        Returns:
            bool: True if connection should be kept open (pending query), False otherwise
        """
        agency = Protocol.parse_query_winners(msg)
        with self._state_lock:
            if not self._lottery_completed:
                self._pending_winners_queries.append((client_sock, agency))
                logging.info(
                    f"action: query_winners_pending | result: in_progress | agency: {agency} "
                    f"| pending_count: {len(self._pending_winners_queries)}"
                )
                with self._connections_lock:
                    self._active_connections.discard(client_sock)
                return True
            else:
                winners = self._get_winners_for_agency(agency)
        
        winners_msg = Protocol.serialize_winners(winners)
        self.__send_complete_message(client_sock, winners_msg.encode())
        logging.info(f'action: winners_sent | result: success | agency: {agency} | count: {len(winners)}')
        return False
    
    def __handle_protocol_error(self, msg: str, error: ProtocolError, client_sock):
        """
        Handle protocol errors by logging appropriate messages and sending error response.
        
        Args:
            msg (str): The original message that caused the error
            error (ProtocolError): The protocol error that occurred
            client_sock: The client socket to send error response to
        """
        if msg.startswith("BATCH#"):
            try:
                header_line = msg.split('\n')[0]
                count = int(header_line.split('#')[1])
                logging.error(f'action: apuesta_recibida | result: fail | cantidad: {count}')
            except:
                logging.error(f'action: apuesta_recibida | result: fail | cantidad: unknown')
        elif msg.startswith("BET#"):
            logging.error(f'action: apuesta_recibida | result: fail | cantidad: 1')
        elif msg.startswith("FINISH_BETS#"):
            logging.error(f'action: finish_bets_received | result: fail | error: {error}')
        elif msg.startswith("QUERY_WINNERS#"):
            logging.error(f'action: query_winners | result: fail | error: {error}')
        else:
            logging.error(f'action: unknown_message | result: fail | error: {error}')
        
        self.__send_complete_message(client_sock, b"ERROR\n")
    
    def __handle_connection_cleanup(self, client_sock, keep_open: bool):
        """
        Handle cleanup operations for client connection.
        
        Args:
            client_sock: The client socket to clean up
            keep_open (bool): Whether to keep the socket open
        """
        with self._connections_lock:
            self._active_connections.discard(client_sock)
        
        if not keep_open:
            try:
                client_sock.close()
                logging.info("action: close_client_connection | result: success")
            except OSError as e:
                logging.warning(f"action: close_client_connection | result: fail | error: {e}")
        
        with self._threads_lock:
            self._threads.discard(threading.current_thread())
    
    def _process_pending_winner_queries(self):
        """
        Process all pending winner queries after the lottery is completed.
        """
        logging.info(f"action: processing_pending_queries | result: success | count: {len(self._pending_winners_queries)}")
        
        for client_sock, agency in self._pending_winners_queries:
            try:
                winners = self._get_winners_for_agency(agency)
                winners_msg = Protocol.serialize_winners(winners)
                self.__send_complete_message(client_sock, winners_msg.encode())
                logging.info(f'action: winners_sent | result: success | agency: {agency} | count: {len(winners)}')
                
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  
                client_sock.close()
            except Exception as e:
                logging.error(f'action: send_pending_winners | result: fail | agency: {agency} | error: {e}')
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    client_sock.close()
                except:
                    pass
        
        self._pending_winners_queries.clear()
        
    