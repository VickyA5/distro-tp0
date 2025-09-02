import socket
import logging
import signal
from common.protocol import Protocol, ProtocolError
from common.utils import store_bets, load_bets, has_won



class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._shutdown_requested = False
        self._active_connections = set()
        
        # Lottery state management
        self._agencies_finished = set()  # Track which agencies finished sending bets
        self._agencies_that_sent_bets = set()  # Track which agencies sent at least one bet
        self._total_agencies = 5  # Default to 5, but will be adjusted dynamically
        self._lottery_completed = False  # Whether the lottery draw has been completed
        self._pending_winners_queries = []  # Store pending winner queries until lottery completes

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
        logging.info(f'action: signal_received | result: success | signal: {signal_name} | active_connections: {len(self._active_connections)} | shutdown: graceful')
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
            logging.info(f'action: receive_message | result: success | ip: {addr[0]} | msg_size: {len(msg)} bytes | msg_type: {msg.split("#")[0] if "#" in msg else msg.split()[0] if msg else "empty"}')

            try:
                if msg.startswith("BATCH#"):
                    bets = Protocol.parse_batch(msg)
                    store_bets(bets)
                    # Track agencies that send bets
                    for bet in bets:
                        self._agencies_that_sent_bets.add(str(bet.agency))
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: {len(bets)}')
                    
                    self.__send_complete_message(client_sock, b"OK\n")
                    
                elif msg.startswith("BET#"):
                    bet = Protocol.parse_bet(msg)
                    store_bets([bet])
                    # Track agencies that send bets
                    self._agencies_that_sent_bets.add(str(bet.agency))
                    logging.info(f'action: apuesta_recibida | result: success | cantidad: 1')
                    
                    self.__send_complete_message(client_sock, b"OK\n")

                elif msg.startswith("FINISH_BETS#"):
                    agency = Protocol.parse_finish_bets(msg)
                    self._agencies_finished.add(agency)
                    logging.info(f'action: finish_bets_received | result: success | agency: {agency} | agencies_finished: {len(self._agencies_finished)}/{len(self._agencies_that_sent_bets)} | agencies_with_bets: {self._agencies_that_sent_bets}')
                    
                    # Check if all agencies that sent bets have finished
                    if len(self._agencies_finished) == len(self._agencies_that_sent_bets) and not self._lottery_completed and len(self._agencies_that_sent_bets) > 0:
                        self._lottery_completed = True
                        logging.info('action: sorteo | result: success')
                        # Process any pending winner queries
                        self._process_pending_winner_queries()
                    
                    self.__send_complete_message(client_sock, b"OK\n")

                elif msg.startswith("QUERY_WINNERS#"):
                    agency = Protocol.parse_query_winners(msg)
                    
                    if not self._lottery_completed:
                        # Lottery not completed yet, store the query for later processing
                        self._pending_winners_queries.append((client_sock, agency))
                        logging.info(f'action: query_winners_pending | agency: {agency} | pending_count: {len(self._pending_winners_queries)}')
                        # Don't send response yet - keep connection open
                        self._active_connections.discard(client_sock)  # Don't close this connection in finally block
                        return  # Exit without closing the connection
                    else:
                        # Get winners for this agency
                        winners = self._get_winners_for_agency(agency)
                        winners_msg = Protocol.serialize_winners(winners)
                        self.__send_complete_message(client_sock, winners_msg.encode())
                        logging.info(f'action: winners_sent | result: success | agency: {agency} | count: {len(winners)}')

                else:
                    raise ProtocolError("unknown_message_type")
                    
            except ProtocolError as e:
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
                    logging.error(f'action: finish_bets_received | result: fail | error: {e}')
                elif msg.startswith("QUERY_WINNERS#"):
                    logging.error(f'action: query_winners | result: fail | error: {e}')
                else:
                    logging.error(f'action: unknown_message | result: fail | error: {e}')
                
                # Send error response
                self.__send_complete_message(client_sock, b"ERROR\n")

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
                
                # Handle BATCH messages - need to receive all expected BET lines
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
                    # For simple messages (BET, FINISH_BETS, QUERY_WINNERS), just wait for newline
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
                # Check if bet is from the requested agency and is a winner
                if str(bet.agency) == agency and has_won(bet):
                    winners.append(bet.document)
        except Exception as e:
            logging.error(f'action: get_winners | result: fail | agency: {agency} | error: {e}')
        
        return winners
    
    def _process_pending_winner_queries(self):
        """
        Process all pending winner queries after the lottery is completed.
        """
        logging.info(f'action: processing_pending_queries | count: {len(self._pending_winners_queries)}')
        
        for client_sock, agency in self._pending_winners_queries:
            try:
                # Get winners for this agency
                winners = self._get_winners_for_agency(agency)
                winners_msg = Protocol.serialize_winners(winners)
                self.__send_complete_message(client_sock, winners_msg.encode())
                logging.info(f'action: winners_sent | result: success | agency: {agency} | count: {len(winners)}')
                
                # Properly shutdown and close the connection
                try:
                    client_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass  # Connection might already be closed by client
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
        
        # Clear the pending queries
        self._pending_winners_queries.clear()
    