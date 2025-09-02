#!/usr/bin/env python3
import socket
import time

def test_finish_bets():
    """Test manual para FINISH_BETS"""
    print("Conectando al servidor...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 12346))
    
    # Enviar mensaje FINISH_BETS
    msg = "FINISH_BETS#1\n"
    print(f"Enviando: {repr(msg)}")
    sock.send(msg.encode())
    
    # Recibir respuesta
    response = sock.recv(1024).decode()
    print(f"Respuesta: {repr(response)}")
    
    sock.close()

def test_query_winners():
    """Test manual para QUERY_WINNERS"""
    print("Conectando al servidor...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', 12346))
    
    # Enviar mensaje QUERY_WINNERS
    msg = "QUERY_WINNERS#1\n"
    print(f"Enviando: {repr(msg)}")
    sock.send(msg.encode())
    
    # Recibir respuesta
    response = sock.recv(1024).decode()
    print(f"Respuesta: {repr(response)}")
    
    sock.close()

if __name__ == "__main__":
    print("=== Test FINISH_BETS ===")
    try:
        test_finish_bets()
    except Exception as e:
        print(f"Error: {e}")
    
    time.sleep(1)
    
    print("\n=== Test QUERY_WINNERS ===")
    try:
        test_query_winners()
    except Exception as e:
        print(f"Error: {e}")
