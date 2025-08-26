import sys

def generate_docker_compose_content(num_clients):
    """
    Generates the content of the Docker Compose file as a string
    """
    content = "name: tp0\n"
    content += "services:\n"
    
    content += "  server:\n"
    content += "    container_name: server\n"
    content += "    image: server:latest\n"
    content += "    entrypoint: python3 /main.py\n"
    content += "    environment:\n"
    content += "      - PYTHONUNBUFFERED=1\n"
    content += "      - LOGGING_LEVEL=DEBUG\n"
    content += "    networks:\n"
    content += "      - testing_net\n"
    content += "\n"
    
    for i in range(1, num_clients + 1):
        client_name = f"client{i}"
        content += f"  {client_name}:\n"
        content += f"    container_name: {client_name}\n"
        content += "    image: client:latest\n"
        content += "    entrypoint: /client\n"
        content += "    environment:\n"
        content += f"      - CLI_ID={i}\n"
        content += "      - CLI_LOG_LEVEL=DEBUG\n"
        content += "    networks:\n"
        content += "      - testing_net\n"
        content += "    depends_on:\n"
        content += "      - server\n"
        content += "\n"
    
    content += "networks:\n"
    content += "  testing_net:\n"
    content += "    ipam:\n"
    content += "      driver: default\n"
    content += "      config:\n"
    content += "        - subnet: 172.25.125.0/24\n"
    
    return content

def write_docker_compose(output_file, content):
    """
    Writes the Docker Compose content to the specified file
    """
    with open(output_file, 'w') as file:
        file.write(content)

def main():
    """
    Main function that processes arguments and generates the Docker Compose file
    """
    if len(sys.argv) != 3:
        print("Usage: python3 generador-compose.py <output_file> <num_clients>")
        sys.exit(1)
    
    output_file = sys.argv[1]
    try:
        num_clients = int(sys.argv[2])
        if num_clients <= 0:
            raise ValueError("Client number must be a positive integer")
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    content = generate_docker_compose_content(num_clients)

    write_docker_compose(output_file, content)

    print(f"Docker Compose file generated: {output_file}")
    print(f"Number of clients: {num_clients}")

if __name__ == "__main__":
    main()
